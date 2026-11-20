import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientHandler implements Runnable {

    private static final int DEFAULT_HANDSHAKE_TIMEOUT_MS = 30_000;
    private static final int MAX_USERNAME_LENGTH = 32;
    private static final int MAX_MESSAGE_LENGTH = 4096;

    private final Socket socket;
    private final Set<ClientHandler> clientHandlers;
    private final int handshakeTimeoutMs;
    private final Object writeLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile boolean registered = false;
    private volatile String clientUsername;
    private volatile BufferedReader bufferedReader;
    private volatile BufferedWriter bufferedWriter;
    private boolean skipNextLF = false;

    public ClientHandler(Socket socket) {
        this(socket, ConcurrentHashMap.newKeySet(), DEFAULT_HANDSHAKE_TIMEOUT_MS);
    }

    public ClientHandler(Socket socket, Set<ClientHandler> clientHandlers) {
        this(socket, clientHandlers, DEFAULT_HANDSHAKE_TIMEOUT_MS);
    }

    public ClientHandler(Socket socket, Set<ClientHandler> clientHandlers, int handshakeTimeoutMs) {
        this.socket = Objects.requireNonNull(socket, "socket must not be null");
        this.clientHandlers = Objects.requireNonNull(clientHandlers, "clientHandlers must not be null");
        if (handshakeTimeoutMs < 0) {
            throw new IllegalArgumentException("handshakeTimeoutMs must be non-negative: " + handshakeTimeoutMs);
        }
        this.handshakeTimeoutMs = handshakeTimeoutMs;
    }

    @Override
    public void run() {
        try {
            if (closed.get() || socket.isClosed()) {
                return;
            }
            this.bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            this.bufferedReader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            if (handshakeTimeoutMs > 0) {
                socket.setSoTimeout(handshakeTimeoutMs);
            }

            String rawUsername;
            try {
                rawUsername = readLineBounded(bufferedReader, MAX_USERNAME_LENGTH + 64);
            } catch (SocketTimeoutException e) {
                sendMessage("SERVER: Username handshake timed out.");
                return;
            }

            if (rawUsername == null) {
                return;
            }

            String validatedUsername = validateUsername(rawUsername);
            if (validatedUsername == null) {
                sendMessage("SERVER: Invalid username. Must be 1-32 printable characters and cannot be 'SERVER'.");
                return;
            }

            List<ClientHandler> existingPeers = tryRegister(validatedUsername);
            if (existingPeers == null) {
                sendMessage("SERVER: Username '" + validatedUsername + "' is already taken.");
                return;
            }

            socket.setSoTimeout(0);
            broadcastToTargets("SERVER: " + clientUsername + " has entered the chat!", existingPeers);

            while (!closed.get() && !socket.isClosed()) {
                String messageFromClient = readLineBounded(bufferedReader, MAX_MESSAGE_LENGTH);
                if (messageFromClient == null) {
                    break;
                }
                String trimmed = messageFromClient.trim();
                if ("/quit".equalsIgnoreCase(trimmed) || "/exit".equalsIgnoreCase(trimmed)) {
                    break;
                }
                String sanitized = sanitizeMessage(messageFromClient);
                if (sanitized.trim().isEmpty()) {
                    continue;
                }
                broadcastMessage(clientUsername + ": " + sanitized, this);
            }
        } catch (IOException ignored) {
            // Connection closed, reset, or exceeded max line length
        } finally {
            closeEverything();
        }
    }

    public boolean sendMessage(String message) {
        if (message == null || closed.get()) {
            return false;
        }
        synchronized (writeLock) {
            if (closed.get() || bufferedWriter == null) {
                return false;
            }
            try {
                bufferedWriter.write(message);
                bufferedWriter.newLine();
                bufferedWriter.flush();
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    private void broadcastMessage(String messageToSend, ClientHandler sender) {
        if (messageToSend == null) {
            return;
        }
        for (ClientHandler recipient : clientHandlers) {
            if (recipient != sender) {
                if (!recipient.sendMessage(messageToSend)) {
                    recipient.closeEverything();
                }
            }
        }
    }

    private void broadcastToTargets(String messageToSend, Iterable<ClientHandler> targets) {
        if (messageToSend == null || targets == null) {
            return;
        }
        for (ClientHandler recipient : targets) {
            if (recipient != this) {
                if (!recipient.sendMessage(messageToSend)) {
                    recipient.closeEverything();
                }
            }
        }
    }

    private List<ClientHandler> tryRegister(String username) {
        synchronized (clientHandlers) {
            if (closed.get()) {
                return null;
            }
            for (ClientHandler existing : clientHandlers) {
                String existingName = existing.clientUsername;
                if (existingName != null && existingName.equalsIgnoreCase(username)) {
                    return null;
                }
            }
            List<ClientHandler> existingPeers = new ArrayList<>(clientHandlers);
            this.clientUsername = username;
            this.registered = true;
            clientHandlers.add(this);
            return existingPeers;
        }
    }

    private static String validateUsername(String rawUsername) {
        if (rawUsername == null) {
            return null;
        }
        String trimmed = rawUsername.trim();
        if (trimmed.isEmpty() || trimmed.length() > MAX_USERNAME_LENGTH) {
            return null;
        }
        if ("SERVER".equalsIgnoreCase(trimmed)) {
            return null;
        }
        for (int i = 0; i < trimmed.length(); i++) {
            if (Character.isISOControl(trimmed.charAt(i))) {
                return null;
            }
        }
        return trimmed;
    }

    private static String sanitizeMessage(String rawMessage) {
        StringBuilder sb = new StringBuilder(rawMessage.length());
        for (int i = 0; i < rawMessage.length(); i++) {
            char c = rawMessage.charAt(i);
            if (!Character.isISOControl(c)) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private String readLineBounded(BufferedReader reader, int maxLength) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int ch = reader.read();
            if (ch == -1) {
                return sb.length() == 0 ? null : sb.toString();
            }
            if (skipNextLF) {
                skipNextLF = false;
                if (ch == '\n') {
                    continue;
                }
            }
            if (ch == '\n') {
                return sb.toString();
            }
            if (ch == '\r') {
                skipNextLF = true;
                return sb.toString();
            }
            if (sb.length() >= maxLength) {
                throw new IOException("Line exceeded maximum allowed length of " + maxLength);
            }
            sb.append((char) ch);
        }
    }

    public void closeEverything() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        boolean shouldBroadcastLeave = false;
        synchronized (clientHandlers) {
            boolean removed = clientHandlers.remove(this);
            if (removed && registered && clientUsername != null) {
                shouldBroadcastLeave = true;
            }
        }
        if (shouldBroadcastLeave) {
            broadcastMessage("SERVER: " + clientUsername + " has left the chat!", this);
        }
        closeQuietly(socket);
        closeQuietly(bufferedWriter);
        closeQuietly(bufferedReader);
    }

    private static void closeQuietly(Closeable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (IOException ignored) {
                // Ignore close failure during teardown
            }
        }
    }
}
