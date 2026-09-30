package com.example;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Collections;
import java.util.concurrent.locks.LockSupport;

public class Common {

    // ====== Cấu hình chung ======
    // SSM dùng dải 232.x.x.x. Nếu join SSM bị lỗi trên máy bạn, đổi USE_SSM = false (dùng ASM 239.x.x.x)
    public static final boolean USE_SSM = true;
    public static final String GROUP = USE_SSM ? "232.1.1.1" : "239.1.1.1";
    public static final int PORT = 5000;          // cổng multicast (data)
    public static final int NACK_PORT = 5001;     // cổng sender nhận NACK (dùng ở Part 2)
    public static final int CHUNK = 1400;         // payload mỗi gói, tránh phân mảnh IP
    public static final int UDP_IP_OVERHEAD = 28; // 20 byte IP + 8 byte UDP

    // ====== Header gói tin (dùng ở Part 2): type(1) + seq(4) + total(4) ======
    public static final byte DATA = 1, NACK = 2, END = 3;
    public static final int HEADER = 9;

    public static void putHeader(ByteBuffer b, byte type, int seq, int total) {
        b.put(type);
        b.putInt(seq);
        b.putInt(total);
    }

    // ====== Chọn network interface (KHÔNG dùng loopback) ======
    public static class Net {
        public NetworkInterface ni;
        public Inet4Address ip;
    }

    // Có thể ép interface bằng VM option: -Diface=wlan0 (hoặc 1 phần tên hiển thị, vd -Diface=Wi-Fi)
    public static Net pickInterface() throws SocketException {
        String want = System.getProperty("iface");
        for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            if (!ni.isUp() || ni.isLoopback() || !ni.supportsMulticast()) continue;
            if (want != null && !ni.getName().equals(want) && !ni.getDisplayName().contains(want)) continue;
            for (InetAddress a : Collections.list(ni.getInetAddresses())) {
                if (a instanceof Inet4Address v4 && !v4.isLinkLocalAddress()) {
                    Net n = new Net();
                    n.ni = ni;
                    n.ip = v4;
                    return n;
                }
            }
        }
        throw new IllegalStateException("Không tìm thấy interface phù hợp (cần đang kết nối mạng, có IPv4)");
    }

    // ====== Kênh gửi: dùng cho cả multicast lẫn unicast ======
    // Bind vào IP của interface -> đây chính là "source address" cho SSM
    public static DatagramChannel openSender(Net net) throws IOException {
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET);
        ch.setOption(StandardSocketOptions.IP_MULTICAST_IF, net.ni);
        ch.setOption(StandardSocketOptions.IP_MULTICAST_TTL, 1);
        ch.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true); // để process cùng máy nhận được
        ch.bind(new InetSocketAddress(net.ip, 0));
        return ch;
    }

    // ====== Kênh nhận multicast: nhiều receiver cùng máy dùng chung PORT nhờ SO_REUSEADDR ======
    public static DatagramChannel openMulticastReceiver(Net net) throws IOException {
        DatagramChannel ch = DatagramChannel.open(StandardProtocolFamily.INET);
        ch.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ch.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024 * 1024);
        ch.bind(new InetSocketAddress(PORT));
        ch.setOption(StandardSocketOptions.IP_MULTICAST_IF, net.ni);
        InetAddress group = InetAddress.getByName(GROUP);
        if (USE_SSM) {
            ch.join(group, net.ni, net.ip);   // IGMPv3 source-specific join (chỉ nhận từ net.ip)
        } else {
            ch.join(group, net.ni);           // ASM join thường
        }
        return ch;
    }

    // ====== Giới hạn tốc độ gửi: chờ tới khi đủ thời gian cho bytesSoFar ở tốc độ mbps ======
    public static void paceTo(long startNanos, long bytesSoFar, double mbps) {
        long target = startNanos + (long) (bytesSoFar * 8 / (mbps * 1e6) * 1e9);
        long wait;
        while ((wait = target - System.nanoTime()) > 0) {
            if (wait > 1_000_000) LockSupport.parkNanos(wait - 500_000);
            // còn dưới 1ms thì spin cho chính xác
        }
    }
}