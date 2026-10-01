package com.example;

import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Random;

/**
 * Part 2 - Receiver.
 * Args: [id = 1] [drop = 0]
 * Luồng: nhận DATA -> đánh dấu gói đã có -> phát hiện thiếu (nhảy cóc seq / im lặng / nhận END)
 *        -> chờ backoff ngẫu nhiên 0-100ms -> gửi NACK unicast về sender -> nhận repair -> đủ thì ghép file + SHA-256.
 */
public class Receiver {

    public static void main(String[] args) throws Exception {
        int id = args.length > 0 ? Integer.parseInt(args[0]) : 1;
        double drop = args.length > 1 ? Double.parseDouble(args[1]) : 0;

        Common.Net net = Common.pickInterface();
        DatagramChannel ch = Common.openMulticastReceiver(net);
        ch.configureBlocking(false);
        Selector sel = Selector.open();
        ch.register(sel, SelectionKey.OP_READ);

        DatagramChannel nackOut = DatagramChannel.open(StandardProtocolFamily.INET);
        InetSocketAddress senderNack = new InetSocketAddress(net.ip, Common.NACK_PORT);

        Random rnd = new Random();
        ByteBuffer buf = ByteBuffer.allocate(2048);

        byte[][] chunks = null;               // chunks[seq] != null nghĩa là đã nhận
        int total = -1, count = 0, highest = -1;
        boolean endSeen = false;
        long lastPkt = System.nanoTime(), firstPkt = 0;
        long nackAt = 0, cooldownUntil = 0;   // nackAt = 0: chưa hẹn giờ gửi NACK
        int dropped = 0, nacksSent = 0;

        System.out.printf("[R%d] Sẵn sàng, cấu hình drop = %.0f%%. Chờ sender gửi dữ liệu...%n", id, drop * 100);

        while (true) {
            sel.select(20);
            sel.selectedKeys().clear();

            // ---- đọc hết các gói đang có ----
            buf.clear();
            while (ch.receive(buf) != null) {
                buf.flip();
                byte type = buf.get();
                int seq = buf.getInt();
                int tot = buf.getInt();
                long t = System.nanoTime();

                if (total < 0 && (type == Common.DATA || type == Common.END)) {
                    total = tot;
                    chunks = new byte[total][];
                    firstPkt = t;
                }
                if (type == Common.END) {
                    endSeen = true;
                } else if (type == Common.DATA) {
                    lastPkt = t;
                    if (rnd.nextDouble() < drop) {
                        dropped++;                       // giả lập mất gói
                    } else if (seq < total && chunks[seq] == null) {
                        byte[] p = new byte[buf.remaining()];
                        buf.get(p);
                        chunks[seq] = p;
                        count++;
                        if (seq > highest) highest = seq;
                    }
                }
                buf.clear();
            }

            long now = System.nanoTime();
            if (total > 0 && count == total) break;   // đủ gói

            if (now - lastPkt > 30_000_000_000L) {
                System.out.printf("[R%d] 30s không nhận thêm gói nào, thoát (còn thiếu %d gói)%n",
                        id, total < 0 ? -1 : total - count);
                return;
            }

            // ---- phát hiện thiếu + gửi NACK ----
            if (total > 0) {
                boolean idle = now - lastPkt > 300_000_000L;              // mạng im 300ms
                int limit = (idle || endSeen) ? total : highest + 1;      // thiếu tới đâu thì NACK tới đó

                if (nackAt == 0 && now >= cooldownUntil && hasMissing(chunks, limit)) {
                    nackAt = now + (long) (rnd.nextDouble() * 100_000_000L);   // backoff ngẫu nhiên 0-100ms
                }
                if (nackAt != 0 && now >= nackAt) {
                    int missing = sendNack(nackOut, senderNack, chunks, limit, id);
                    if (missing > 0) {
                        nacksSent++;
                        System.out.printf("[R%d] Gửi NACK: thiếu %d gói (đã nhận %d/%d)%n", id, missing, count, total);
                    }
                    nackAt = 0;
                    cooldownUntil = now + 300_000_000L;   // chờ repair về rồi mới NACK tiếp
                }
            }
        }

        // ---- ghép file + kiểm tra hash ----
        double sec = (System.nanoTime() - firstPkt) / 1e9;
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        String outFile = "received_" + id + ".bin";
        try (FileOutputStream fo = new FileOutputStream(outFile)) {
            for (byte[] c : chunks) {
                fo.write(c);
                md.update(c);
            }
        }
        System.out.printf("%n[R%d] ===== HOÀN TẤT =====%n", id);
        System.out.printf("[R%d] Cấu hình drop = %.0f%% (tương ứng %d gói) | NACK đã gửi: %d | thời gian: %.1f s%n",
                id, drop*100, dropped, nacksSent, sec);
        System.out.printf("[R%d] SHA-256: %s%n", id, HexFormat.of().formatHex(md.digest()));
    }

    static boolean hasMissing(byte[][] chunks, int limit) {
        for (int i = 0; i < limit; i++) {
            if (chunks[i] == null) return true;
        }
        return false;
    }

    // Gom các seq thiếu thành range [start, end], gửi unicast về sender. Trả về số gói thiếu.
    static int sendNack(DatagramChannel ch, InetSocketAddress dst, byte[][] chunks, int limit, int id) throws Exception {
        List<int[]> ranges = new ArrayList<>();
        int missing = 0;
        int i = 0;
        while (i < limit) {
            if (chunks[i] == null) {
                int s = i;
                while (i < limit && chunks[i] == null) i++;
                ranges.add(new int[]{s, i - 1});
                missing += i - s;
            } else {
                i++;
            }
        }
        if (ranges.isEmpty()) return 0;

        int n = Math.min(ranges.size(), 170);     // vừa 1 gói UDP; phần còn lại NACK ở vòng sau
        ByteBuffer nb = ByteBuffer.allocate(Common.HEADER + n * 8);
        Common.putHeader(nb, Common.NACK, id, n);
        for (int k = 0; k < n; k++) {
            nb.putInt(ranges.get(k)[0]);
            nb.putInt(ranges.get(k)[1]);
        }
        nb.flip();
        ch.send(nb, dst);
        return missing;
    }
}