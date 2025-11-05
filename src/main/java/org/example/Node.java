package org.example;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node {
    private final int myId;
    private final Map<Integer, InetSocketAddress> nodes = new ConcurrentHashMap<>();
    private volatile boolean isCoordinator = false;
    private volatile int currentCoordinator = -1;
    private final ServerSocket serverSocket;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean gotOk = new AtomicBoolean(false);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService handlerPool = Executors.newCachedThreadPool();
    private static final int SOCKET_TIMEOUT_MS = 3000;
    private static final int ELECTION_WAIT_MS = 3000;
    private static final int HEARTBEAT_INTERVAL_MS = 2000;
    private volatile long lastHeartbeatTs = System.currentTimeMillis();

    public Node(int myId, String configPath) throws IOException {
        this.myId = myId;
        loadConfig(configPath);
        InetSocketAddress me = nodes.get(myId);
        if (me == null) throw new IllegalArgumentException("My id not found in config");

        ServerSocket tmp = null;
        try {
            tmp = new ServerSocket(me.getPort(), 50, me.getAddress());
            System.out.println("Node " + myId + " bound to configured address " + me.getAddress() + ":" + me.getPort());
        } catch (IOException e) {
            // fallback para todas as interfaces (0.0.0.0)
            tmp = new ServerSocket(me.getPort());
            System.out.println("Node " + myId + " could not bind to " + me.getAddress() + ". Falling back to 0.0.0.0:" + me.getPort());
        }
        this.serverSocket = tmp;

        System.out.println("Node " + myId + " listening on " + serverSocket.getInetAddress() + ":" + serverSocket.getLocalPort());
        System.out.println("Known nodes:");
        for (Map.Entry<Integer, InetSocketAddress> e : nodes.entrySet()) {
            System.out.println("  id=" + e.getKey() + " addr=" + e.getValue().getAddress() + " port=" + e.getValue().getPort());
        }

        startServer();
        startHeartbeatChecker();
    }

    private void loadConfig(String path) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) continue;
                String[] parts = line.split("\\s+");
                if (parts.length < 3) continue;
                int id = Integer.parseInt(parts[0]);
                InetAddress addr = InetAddress.getByName(parts[1]);
                int port = Integer.parseInt(parts[2]);
                nodes.put(id, new InetSocketAddress(addr, port));
            }
        }
    }

    private void startServer() {
        Thread t = new Thread(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    Socket s = serverSocket.accept();
                    s.setSoTimeout(SOCKET_TIMEOUT_MS);
                    System.out.println("Accepted connection from " + s.getRemoteSocketAddress());
                    handlerPool.submit(() -> handleConnection(s));
                } catch (IOException e) {
                    // aceitar falhas (podem ocorrer no fechamento)
                    if (!serverSocket.isClosed()) {
                        System.err.println("Accept error: " + e.getMessage());
                    }
                }
            }
        }, "server-accept-thread");
        t.setDaemon(true);
        t.start();
    }

    private void handleConnection(Socket s) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             PrintWriter out = new PrintWriter(s.getOutputStream(), true)) {
            String line = in.readLine();
            if (line == null) return;
            System.out.println("Received: " + line + " from " + s.getRemoteSocketAddress());
            Message msg = Message.parse(line);
            if (msg == null) return;
            switch (msg.type) {
                case "ELECTION":
                    out.println(Message.format("OK", myId));
                    System.out.println("Sent OK to " + msg.senderId);
                    if (!electionInProgress.get()) {
                        scheduler.submit(this::startElection);
                    }
                    break;
                case "OK":
                    gotOk.set(true);
                    System.out.println("Got OK from " + msg.senderId);
                    break;
                case "COORDINATOR":
                    int coord = msg.senderId;
                    currentCoordinator = coord;
                    isCoordinator = (coord == myId);
                    electionInProgress.set(false);
                    lastHeartbeatTs = System.currentTimeMillis();
                    System.out.println("Coordinator announced: " + coord);
                    break;
                case "HEARTBEAT":
                    currentCoordinator = msg.senderId;
                    lastHeartbeatTs = System.currentTimeMillis();
                    // opcional: log curto
                    // System.out.println("Heartbeat from " + msg.senderId);
                    break;
                default:
                    // ignorar
            }
        } catch (IOException ignored) {
        } finally {
            try { s.close(); } catch (IOException ignored) {}
        }
    }

    private void sendTo(int nodeId, String payload) {
        InetSocketAddress addr = nodes.get(nodeId);
        if (addr == null) return;
        try (Socket socket = new Socket()) {
            System.out.println("Sending '" + payload + "' to id=" + nodeId + " addr=" + addr);
            socket.connect(addr, SOCKET_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(payload);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String resp = in.readLine();
            if (resp != null) {
                Message r = Message.parse(resp);
                if (r != null && "OK".equals(r.type)) {
                    gotOk.set(true);
                }
            }
        } catch (IOException e) {
            // falha ao conectar - log curto
            System.err.println("Send to " + nodeId + " failed: " + e.getMessage());
        }
    }

    public void start() {
        scheduler.schedule(() -> {
            if (currentCoordinator == -1) {
                startElection();
            }
        }, 1, TimeUnit.SECONDS);
    }

    private void startHeartbeatChecker() {
        scheduler.scheduleAtFixedRate(() -> {
            if (isCoordinator) {
                broadcast(Message.format("HEARTBEAT", myId));
            } else {
                long now = System.currentTimeMillis();
                if (now - lastHeartbeatTs > 3 * HEARTBEAT_INTERVAL_MS) {
                    if (electionInProgress.compareAndSet(false, true)) {
                        System.out.println("Suspecting coordinator failure, starting election");
                        scheduler.submit(this::startElection);
                    }
                }
            }
        }, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        try {
            gotOk.set(false);
            int higherCount = 0;
            for (int id : nodes.keySet()) {
                if (id > myId) {
                    higherCount++;
                    final int target = id;
                    handlerPool.submit(() -> sendTo(target, Message.format("ELECTION", myId)));
                }
            }
            if (higherCount == 0) {
                becomeCoordinator();
                return;
            }
            long waited = 0;
            while (waited < ELECTION_WAIT_MS) {
                if (gotOk.get()) break;
                Thread.sleep(200);
                waited += 200;
            }
            if (gotOk.get()) {
                long waitForCoord = 0;
                while (waitForCoord < ELECTION_WAIT_MS) {
                    if (!electionInProgress.get()) return;
                    if (currentCoordinator != -1 && currentCoordinator != myId) {
                        electionInProgress.set(false);
                        return;
                    }
                    Thread.sleep(200);
                    waitForCoord += 200;
                }
                electionInProgress.set(false);
                startElection();
            } else {
                becomeCoordinator();
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private void becomeCoordinator() {
        isCoordinator = true;
        currentCoordinator = myId;
        electionInProgress.set(false);
        System.out.println("Becoming coordinator: " + myId);
        broadcast(Message.format("COORDINATOR", myId));
    }

    private void broadcast(String payload) {
        for (int id : nodes.keySet()) {
            if (id == myId) continue;
            handlerPool.submit(() -> sendTo(id, payload));
        }
    }

    public void shutdown() throws IOException {
        scheduler.shutdownNow();
        handlerPool.shutdownNow();
        serverSocket.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: java org.example.Node <myId> <configFile>");
            System.exit(1);
        }
        int myId = Integer.parseInt(args[0]);
        String cfg = args[1];
        Node node = new Node(myId, cfg);
        node.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { node.shutdown(); } catch (IOException ignored) {}
        }));
    }
}
