package org.example;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Node {
    private final int myId;
    private final Map<Integer, InetSocketAddress> nodes = new ConcurrentHashMap<>();
    private volatile boolean isCoordinator = false;
    private volatile int currentCoordinator = -1;
    private final ServerSocket serverSocket;
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicBoolean gotOk = new AtomicBoolean(false);
    private static final int SOCKET_TIMEOUT_MS = 3000;
    private static final int ELECTION_WAIT_MS = 3000;
    private static final int HEARTBEAT_INTERVAL_MS = 2000;
    private volatile long lastHeartbeatTs = System.currentTimeMillis();

    private volatile boolean running = true;
    private Thread acceptThread;
    private Thread heartbeatThread;

    public Node(int myId, String configPath) throws IOException {
        this.myId = myId;
        loadConfig(configPath);

        InetSocketAddress me = nodes.get(myId);
        if (me == null) throw new IllegalArgumentException("O ID do nó não existe nas configurações");

        ServerSocket tmp = null;
        try {
            tmp = new ServerSocket(me.getPort(), 50, me.getAddress());
        } catch (IOException e) {
            tmp = new ServerSocket(me.getPort());
            System.out.println("Nó " + myId + " não conseguiu conectar ao endereço " + me.getAddress());
        }
        this.serverSocket = tmp;

        System.out.println("Nó " + myId + " escutando no endereço " + serverSocket.getInetAddress() + ":" + serverSocket.getLocalPort());
        System.out.println("Nós conhecidos:");
        for (Map.Entry<Integer, InetSocketAddress> e : nodes.entrySet()) {
            System.out.println("  id=" + e.getKey() + " addr=" + e.getValue().getAddress() + " port=" + e.getValue().getPort());
        }

        startServer();
        startHeartbeatChecker();
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
        acceptThread = new Thread(() -> {
            while (running && !serverSocket.isClosed()) {
                try {
                    Socket s = serverSocket.accept();
                    s.setSoTimeout(SOCKET_TIMEOUT_MS);
                    System.out.println("Conexão aceita de " + s.getRemoteSocketAddress());
                    Thread handler = new Thread(() -> handleConnection(s));
                    handler.setDaemon(true);
                    handler.start();
                } catch (IOException e) {
                    if (running && !serverSocket.isClosed()) {
                        System.err.println("Erro ao aceitar conexão: " + e.getMessage());
                    } else {
                        break;
                    }
                }
            }
        }, "server-accept-thread");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    private void handleConnection(Socket s) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true)
        ) {
            String line = in.readLine();
            if (line == null) return;
            System.out.println("Mensagem: " + line + " recebida de " + s.getRemoteSocketAddress());
            Message msg = Message.parse(line);
            if (msg == null) return;
            switch (msg.type) {
                case "ELECTION":
                    out.println(Message.format("OK", myId));
                    System.out.println("OK enviado para " + msg.senderId);
                    if (!electionInProgress.get()) {
                        new Thread(this::startElection, "start-election-from-ELECTION").start();
                    }
                    break;
                case "OK":
                    gotOk.set(true);
                    System.out.println("OK recebido de " + msg.senderId);
                    break;
                case "COORDINATOR":
                    int coord = msg.senderId;
                    currentCoordinator = coord;
                    isCoordinator = (coord == myId);
                    electionInProgress.set(false);
                    lastHeartbeatTs = System.currentTimeMillis();
                    System.out.println("Coordenador anunciado: " + coord);
                    break;
                case "HEARTBEAT":
                    currentCoordinator = msg.senderId;
                    lastHeartbeatTs = System.currentTimeMillis();
                    break;
                case "PING":
                    out.println(Message.format("PONG", myId));
                    System.out.println("PONG enviado para " + msg.senderId);
                    break;
                default:
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
            System.out.println("Enviando '" + payload + "' para id=" + nodeId + " addr=" + addr);
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
            System.err.println("Envio ao nó " + nodeId + " falhou: " + e.getMessage());
        }
    }

    public void start() {
        Thread starter = new Thread(() -> {
            try {
                Thread.sleep(1000);
                if (currentCoordinator == -1) {
                    startElection();
                }
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }, "start-delay");
        starter.setDaemon(true);
        starter.start();
    }

    private void startHeartbeatChecker() {
        heartbeatThread = new Thread(() -> {
            while (running) {
                try {
                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (!isCoordinator) {
                    long now = System.currentTimeMillis();
                    if (currentCoordinator == -1) {
                        if (electionInProgress.compareAndSet(false, true)) {
                            System.out.println("Sem coordenador conhecido, iniciando eleição");
                            Thread t = new Thread(this::startElection, "start-election-no-coord");
                            t.setDaemon(true);
                            t.start();
                        }
                        continue;
                    }
                    pingLeader();
                    if (now - lastHeartbeatTs > 3 * HEARTBEAT_INTERVAL_MS) {
                        if (electionInProgress.compareAndSet(false, true)) {
                            System.out.println("Suspeita de coordenador inativo, iniciando eleição");
                            Thread t = new Thread(this::startElection, "start-election-suspect");
                            t.setDaemon(true);
                            t.start();
                        }
                    }
                }
            }
        }, "heartbeat-thread");
        heartbeatThread.setDaemon(true);
        heartbeatThread.start();
    }

    private void pingLeader() {
        int coord = currentCoordinator;
        if (coord == -1 || coord == myId) return;
        InetSocketAddress addr = nodes.get(coord);
        if (addr == null) {
            currentCoordinator = -1;
            return;
        }
        try (Socket socket = new Socket()) {
            socket.connect(addr, SOCKET_TIMEOUT_MS);
            socket.setSoTimeout(SOCKET_TIMEOUT_MS);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.println(Message.format("PING", myId));
            String resp = in.readLine();
            if (resp != null) {
                Message r = Message.parse(resp);
                if (r != null && "PONG".equals(r.type)) {
                    lastHeartbeatTs = System.currentTimeMillis();
                    currentCoordinator = coord;
                    System.out.println("PONG recebido de " + coord);
                }
            }
        } catch (IOException e) {
            System.err.println("Ping ao coordenador " + coord + " falhou: " + e.getMessage());
        }
    }

    private void startElection() {
        try {
            gotOk.set(false);
            int higherCount = 0;
            for (int id : nodes.keySet()) {
                if (id > myId) {
                    higherCount++;
                    final int target = id;
                    Thread t = new Thread(() -> sendTo(target, Message.format("ELECTION", myId)), "send-ELECTION-to-" + target);
                    t.setDaemon(true);
                    t.start();
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
        System.out.println("Virando coordenador: " + myId);
        broadcast(Message.format("COORDINATOR", myId));
    }

    private void broadcast(String payload) {
        for (int id : nodes.keySet()) {
            if (id == myId) continue;
            Thread t = new Thread(() -> sendTo(id, payload), "broadcast-to-" + id);
            t.setDaemon(true);
            t.start();
        }
    }

    public void shutdown() throws IOException {
        running = false;
        try {
            if (acceptThread != null) acceptThread.interrupt();
            if (heartbeatThread != null) heartbeatThread.interrupt();
        } finally {
            serverSocket.close();
        }
    }
}
