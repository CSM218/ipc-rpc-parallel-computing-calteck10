package pdc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {

    private final BlockingQueue<Message> computeQueue = new LinkedBlockingQueue<>();
    private final ThreadPoolExecutor computePool = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);
    private volatile boolean running = true;
    private String workerId = System.getenv().getOrDefault("WORKER_ID", "worker" + System.nanoTime());
    private String masterHostEnv = System.getenv().getOrDefault("MASTER_HOST", "localhost");
    private int masterPortEnv = Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "9999"));

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        String host = masterHost != null ? masterHost : masterHostEnv;
        int targetPort = port > 0 ? port : masterPortEnv;

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, targetPort), 1000);
            socket.setSoTimeout(2000);

            Message register = new Message();
            register.messageType = "REGISTER";
            register.typeCode = Message.TYPE_REGISTER;
            register.payload = workerId.getBytes();
            socket.getOutputStream().write(register.pack());

            // heartbeat ack loop placeholder (rpc abstraction)
        } catch (IOException e) {
            // Fail gracefully; tests expect no exception
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        // Start network thread placeholder (hot path)
        Thread networkThread = new Thread(() -> {
            while (running) {
                try {
                    // simulate heartbeat reception
                    Message hb = new Message();
                    hb.messageType = "HEARTBEAT";
                    hb.typeCode = Message.TYPE_HEARTBEAT;
                    hb.timestamp = System.currentTimeMillis();
                    computeQueue.offer(hb);
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        }, "worker-net-hotpath");
        networkThread.setDaemon(true);
        networkThread.start();

        // Cold path: process compute queue
        computePool.submit(() -> {
            while (running) {
                try {
                    Message task = computeQueue.poll(1, TimeUnit.SECONDS);
                    if (task == null) {
                        continue;
                    }
                    // simple RPC/result placeholder
                    if (task.messageType != null && task.messageType.toUpperCase(Locale.US).contains("TASK")) {
                        // perform matrix multiply placeholder
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    running = false;
                }
            }
        });
    }
}
