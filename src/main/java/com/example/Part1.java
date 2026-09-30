package com.example;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/*
 * Part 1: so sánh băng thông sender.
 * Mỗi "luồng" gửi cùng 1 lượng dữ liệu với tốc độ cố định RATE_MBPS.
 */
public class Part1 {

    static final int FILE_MB = 5;
    static final double RATE_MBPS = 20;           // tốc độ của mỗi luồng
    static final int[] N_LIST = {1, 5, 10, 50, 100, 1000};   // số client
    static final int UNI_BASE = 6000;             // cổng unicast của receiver i = UNI_BASE + i

    record Result(long sentBytes, double seconds, long receivedBytes) {}

    public static void main(String[] args) throws Exception {
        Common.Net net = Common.pickInterface();
        System.out.println("Interface: " + net.ni.getDisplayName() + " / " + net.ip.getHostAddress()
                + " | " + (Common.USE_SSM ? "SSM " : "ASM ") + Common.GROUP);

        int chunks = FILE_MB * 1024 * 1024 / Common.CHUNK;
        byte[] payload = new byte[Common.CHUNK];
        new Random(1).nextBytes(payload);

        System.out.printf("%n%-4s %-10s %14s %14s %14s %11s%n",
                "N", "Mode", "Sent(MB)", "Rate(Mbps)", "Received(MB)", "Delivered%");

        for (int n : N_LIST) {
            for (boolean multicast : new boolean[]{false, true}) {
                Result r = run(multicast, n, net, chunks, payload);
                double sentMb = r.sentBytes / 1e6;
                double mbps = r.sentBytes * 8 / r.seconds / 1e6;
                double recvMb = r.receivedBytes / 1e6;
                double expected = (double) chunks * Common.CHUNK * n;
                double delivered = 100.0 * r.receivedBytes / expected;
                System.out.printf("%-4d %-10s %14.1f %14.1f %14.1f %10.1f%%%n",
                        n, multicast ? "Multicast" : "Unicast", sentMb, mbps, recvMb, delivered);
                Thread.sleep(500);
            }
        }
        System.out.println("Delivered% < 100 ở multicast là do UDP mất gói");
    }

    static Result run(boolean multicast, int n, Common.Net net, int chunks, byte[] payload) throws Exception {
        AtomicLong received = new AtomicLong();
        List<DatagramChannel> receivers = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(n);

        //Tạo N receiver, mỗi receiver 1 thread
        for (int i = 0; i < n; i++) {
            DatagramChannel ch = multicast
                    ? Common.openMulticastReceiver(net)
                    : openUnicastReceiver(net, UNI_BASE + i);
            receivers.add(ch);
            Thread t = new Thread(() -> {
                ByteBuffer buf = ByteBuffer.allocate(2048);
                ready.countDown();
                try {
                    while (true) {
                        buf.clear();
                        ch.receive(buf);
                        received.addAndGet(buf.position());
                    }
                } catch (IOException e) {
                }
            });
            t.setDaemon(true);
            t.start();
        }
        ready.await();
        Thread.sleep(300);

        //Sender
        DatagramChannel sender = Common.openSender(net);
        InetSocketAddress groupAddr = new InetSocketAddress(Common.GROUP, Common.PORT);
        InetSocketAddress[] uniAddrs = new InetSocketAddress[n];
        for (int i = 0; i < n; i++) uniAddrs[i] = new InetSocketAddress(net.ip, UNI_BASE + i);

        ByteBuffer pkt = ByteBuffer.wrap(payload);
        long sentBytes = 0;
        long start = System.nanoTime();

        for (int k = 0; k < chunks; k++) {
            Common.paceTo(start, (long) k * Common.CHUNK, RATE_MBPS);
            if (multicast) {
                pkt.clear();
                sentBytes += sender.send(pkt, groupAddr) + Common.UDP_IP_OVERHEAD;   // gửi 1 lần
            } else {
                for (int i = 0; i < n; i++) {                                        // gửi lặp N lần
                    pkt.clear();
                    sentBytes += sender.send(pkt, uniAddrs[i]) + Common.UDP_IP_OVERHEAD;
                }
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;

        Thread.sleep(500); // chờ gói cuối tới receiver
        for (DatagramChannel ch : receivers) ch.close();
        sender.close();
        Thread.sleep(100);

        return new Result(sentBytes, seconds, received.get());
    }

    static DatagramChannel openUnicastReceiver(Common.Net net, int port) throws IOException {
        DatagramChannel ch = DatagramChannel.open();
        ch.setOption(StandardSocketOptions.SO_RCVBUF, 4 * 1024 * 1024);
        ch.bind(new InetSocketAddress(net.ip, port));
        return ch;
    }
}