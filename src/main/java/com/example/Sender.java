package com.example;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Part 2 - Sender.
 * Args (tùy chọn): [đường dẫn file = sample.bin] [tốc độ Mbps = 20]
 *
 * Luồng: gửi hết DATA bằng multicast -> gửi vài gói END -> nhận NACK (unicast) -> gửi lại (repair) bằng multicast
 *        -> dừng khi 3 giây không còn NACK nào.
 */
public class Sender {

    static DatagramChannel out;
    static InetSocketAddress group;
    static byte[] data;
    static int total;
    static final ByteBuffer pkt = ByteBuffer.allocate(Common.HEADER + Common.CHUNK);

    // Các seq cần gửi lại. Dùng Set để NACK trùng từ nhiều receiver tự gộp (dedupe)
    static final Set<Integer> pending = ConcurrentHashMap.newKeySet();
    static volatile long lastNack;
    static final AtomicLong nackCount = new AtomicLong();

    public static void main(String[] args) throws Exception {
        String path = args.length > 0 ? args[0] : "sample.bin";
        double rate = args.length > 1 ? Double.parseDouble(args[1]) : 20;

        File f = new File(path);
        if (!f.exists()) {
            byte[] rnd = new byte[3 * 1024 * 1024];
            new Random(42).nextBytes(rnd);
            Files.write(f.toPath(), rnd);
            System.out.println("[SENDER] Chưa có file, đã tạo file mẫu 3MB: " + f.getAbsolutePath());
        }
        data = Files.readAllBytes(f.toPath());
        total = (data.length + Common.CHUNK - 1) / Common.CHUNK;
        String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        System.out.printf("[SENDER] File %d bytes, %d gói, rate %.0f Mbps%n", data.length, total, rate);
        System.out.println("[SENDER] SHA-256: " + hash);

        Common.Net net = Common.pickInterface();
        System.out.println("[SENDER] Interface: " + net.ni.getDisplayName() + " / " + net.ip.getHostAddress()
                + " | " + (Common.USE_SSM ? "SSM " : "ASM ") + Common.GROUP);
        out = Common.openSender(net);
        group = new InetSocketAddress(Common.GROUP, Common.PORT);

        // Kênh nhận NACK (unicast) + thread lắng nghe
        DatagramChannel nackCh = DatagramChannel.open(StandardProtocolFamily.INET);
        nackCh.bind(new InetSocketAddress(net.ip, Common.NACK_PORT));
        Thread nt = new Thread(() -> nackLoop(nackCh));
        nt.setDaemon(true);
        nt.start();

        System.out.println("[SENDER] Chờ 3 giây cho receiver join group...");
        Thread.sleep(3000);

        // ===== Pha 1: gửi toàn bộ dữ liệu gốc =====
        long start = System.nanoTime();
        for (int seq = 0; seq < total; seq++) {
            Common.paceTo(start, (long) seq * Common.CHUNK, rate);
            sendPacket(Common.DATA, seq);
        }
        for (int i = 0; i < 3; i++) {          // báo hiệu hết dữ liệu
            sendPacket(Common.END, 0);
            Thread.sleep(50);
        }
        System.out.println("[SENDER] Đã gửi xong " + total + " gói gốc, chờ NACK...");

        // ===== Pha 2: repair theo NACK =====
        long[] lastRepair = new long[total];
        long perPktNs = (long) (Common.CHUNK * 8 / (rate * 1e6) * 1e9);
        long repairs = 0, lastEnd = 0;
        lastNack = System.nanoTime();

        while (true) {
            List<Integer> todo = new ArrayList<>();
            for (Integer s : pending) {
                if (pending.remove(s)) todo.add(s);
            }
            Collections.sort(todo);

            long now = System.nanoTime();
            for (int s : todo) {
                // Nhiều receiver cùng NACK 1 seq -> chỉ gửi lại 1 lần trong 100ms
                if (lastRepair[s] != 0 && now - lastRepair[s] < 100_000_000L) continue;
                sendPacket(Common.DATA, s);
                repairs++;
                lastRepair[s] = System.nanoTime();
                LockSupport.parkNanos(perPktNs);
            }

            if (todo.isEmpty()) {
                if (now - lastEnd > 500_000_000L) {   // nhắc receiver là đã hết dữ liệu
                    sendPacket(Common.END, 0);
                    lastEnd = now;
                }
                Thread.sleep(20);
                if (now - lastNack > 3_000_000_000L) break;   // 3s không có NACK -> xong
            }
        }

        double sec = (System.nanoTime() - start) / 1e9;
        System.out.println("\n========== KẾT QUẢ SENDER ==========");
        System.out.printf("Gói gốc        : %d%n", total);
        System.out.printf("Gói repair     : %d (%.1f%% so với gói gốc)%n", repairs, 100.0 * repairs / total);
        System.out.printf("Số NACK nhận   : %d%n", nackCount.get());
        System.out.printf("Tổng thời gian : %.1f s%n", sec);
        System.out.println("SHA-256 gốc    : " + hash);
        System.exit(0);
    }

    static void sendPacket(byte type, int seq) throws Exception {
        pkt.clear();
        Common.putHeader(pkt, type, seq, total);
        if (type == Common.DATA) {
            int off = seq * Common.CHUNK;
            pkt.put(data, off, Math.min(Common.CHUNK, data.length - off));
        }
        pkt.flip();
        out.send(pkt, group);
    }

    // NACK: header(type, seq = id receiver, total = số range) + các range [start, end]
    static void nackLoop(DatagramChannel ch) {
        ByteBuffer buf = ByteBuffer.allocate(2048);
        try {
            while (true) {
                buf.clear();
                ch.receive(buf);
                buf.flip();
                if (buf.remaining() < Common.HEADER || buf.get() != Common.NACK) continue;
                int rid = buf.getInt();
                int n = buf.getInt();
                int cnt = 0;
                for (int i = 0; i < n && buf.remaining() >= 8; i++) {
                    int s = buf.getInt(), e = buf.getInt();
                    for (int q = Math.max(s, 0); q <= e && q < total; q++) {
                        pending.add(q);
                        cnt++;
                    }
                }
                lastNack = System.nanoTime();
                nackCount.incrementAndGet();
                System.out.printf("[SENDER] NACK từ R%d: thiếu %d gói%n", rid, cnt);
            }
        } catch (Exception e) {
            // kênh đóng -> thoát
        }
    }
}
