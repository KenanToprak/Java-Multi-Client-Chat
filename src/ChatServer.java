import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ChatServer implements AutoCloseable {

    public static final int DEFAULT_PORT = 1234;
    private static final int MAX_CLIENTS = 128;
    private static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000;

    private final ServerSocket serverSocket;
    private final Set<ClientHandler> clientHandlers = ConcurrentHashMap.newKeySet();
    private final Set<ClientHandler> activeConnections = ConcurrentHashMap.newKeySet();
    private final ExecutorService executorService;
    private final int handshakeTimeoutMs;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ChatServer(int port) throws IOException {
        this(port, DEFAULT_HANDSHAKE_TIMEOUT_MS);
    }

    public ChatServer(int port, int handshakeTimeoutMs) throws IOException {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535: " + port);
        }
        if (handshakeTimeoutMs < 0) {
            throw new IllegalArgumentException("Handshake timeout must be non-negative: " + handshakeTimeoutMs);
        }
        this.serverSocket = new ServerSocket(port);
        this.handshakeTimeoutMs = handshakeTimeoutMs;
        AtomicInteger threadCounter = new AtomicInteger(0);
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "chat-client-" + threadCounter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        this.executorService = new ThreadPoolExecutor(
                0,
                MAX_CLIENTS,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                threadFactory
        );
    }

    public int getLocalPort() {
        return serverSocket.getLocalPort();
    }

    public int getActiveClientCount() {
        return clientHandlers.size();
    }

    public void startServer() throws IOException {
        while (!closed.get() && !serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                if (closed.get()) {
                    closeSocketQuietly(socket);
                    break;
                }
                ClientHandler clientHandler = new ClientHandler(socket, clientHandlers, handshakeTimeoutMs);
                activeConnections.add(clientHandler);
                if (closed.get()) {
                    activeConnections.remove(clientHandler);
                    clientHandler.closeEverything();
                    closeSocketQuietly(socket);
                    break;
                }
                try {
                    executorService.execute(() -> {
                        try {
                            clientHandler.run();
                        } finally {
                            activeConnections.remove(clientHandler);
                        }
                    });
                } catch (RejectedExecutionException rejected) {
                    activeConnections.remove(clientHandler);
                    clientHandler.closeEverything();
                    closeSocketQuietly(socket);
                }
            } catch (SocketException e) {
                if (closed.get() || serverSocket.isClosed()) {
                    break;
                }
                throw e;
            }
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // Best-effort close during shutdown
        }
        executorService.shutdownNow();
        List<ClientHandler> snapshot = new ArrayList<>(activeConnections);
        for (ClientHandler handler : snapshot) {
            handler.closeEverything();
        }
        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeSocketQuietly(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException ignored) {
                // Ignore close error on rejected socket
            }
        }
    }

    public static void main(String[] args) {
        int port = DEFAULT_PORT;
        if (args.length > 1) {
            System.err.println("Usage: java ChatServer [port]");
            System.exit(1);
            return;
        }
        if (args.length == 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[0]);
                System.exit(1);
                return;
            }
            if (port < 1 || port > 65535) {
                System.err.println("Port must be between 1 and 65535: " + port);
                System.exit(1);
                return;
            }
        }

        try (ChatServer server = new ChatServer(port)) {
            Thread shutdownHook = new Thread(server::close, "chat-server-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                System.out.println("Server is running on port " + server.getLocalPort() + "...");
                server.startServer();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM shutdown already in progress
                }
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
            System.exit(1);
        }
    }
}
