import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ChatClient implements AutoCloseable {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 1234;

    private final Socket socket;
    private final BufferedReader bufferedReader;
    private final BufferedWriter bufferedWriter;
    private final String username;
    private final InputStream inputStream;
    private final Consumer<String> outputConsumer;
    private final Object writeLock = new Object();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ChatClient(Socket socket, String username) throws IOException {
        this(socket, username, System.in, System.out::println);
    }

    public ChatClient(Socket socket, String username, InputStream inputStream, Consumer<String> outputConsumer)
            throws IOException {
        if (socket == null) {
            throw new IllegalArgumentException("socket must not be null");
        }
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("username must not be blank");
        }
        String trimmedUsername = username.trim();
        if (trimmedUsername.indexOf('\n') >= 0 || trimmedUsername.indexOf('\r') >= 0) {
            throw new IllegalArgumentException("username must not contain line breaks");
        }
        this.socket = socket;
        this.username = trimmedUsername;
        this.inputStream = Objects.requireNonNull(inputStream, "inputStream must not be null");
        this.outputConsumer = Objects.requireNonNull(outputConsumer, "outputConsumer must not be null");

        BufferedWriter writer = null;
        BufferedReader reader = null;
        try {
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            closeQuietly(socket);
            closeQuietly(writer);
            closeQuietly(reader);
            throw e;
        }
        this.bufferedWriter = writer;
        this.bufferedReader = reader;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public void sendMessage() {
        try {
            if (closed.get() || socket.isClosed()) {
                return;
            }
            synchronized (writeLock) {
                bufferedWriter.write(username);
                bufferedWriter.newLine();
                bufferedWriter.flush();
            }

            BufferedReader inputReader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            while (!closed.get() && !socket.isClosed()) {
                String messageToSend = inputReader.readLine();
                if (messageToSend == null) {
                    break;
                }
                String trimmed = messageToSend.trim();
                if ("/quit".equalsIgnoreCase(trimmed) || "/exit".equalsIgnoreCase(trimmed)) {
                    break;
                }
                if (trimmed.isEmpty()) {
                    continue;
                }
                synchronized (writeLock) {
                    if (closed.get() || socket.isClosed()) {
                        break;
                    }
                    bufferedWriter.write(messageToSend);
                    bufferedWriter.newLine();
                    bufferedWriter.flush();
                }
            }
        } catch (IOException ignored) {
            // Socket or input stream closed
        } finally {
            closeEverything();
        }
    }

    public void listenForMessage() {
        Thread listenerThread = new Thread(() -> {
            try {
                while (!closed.get() && !socket.isClosed()) {
                    String msgFromGroupChat = bufferedReader.readLine();
                    if (msgFromGroupChat == null) {
                        if (!closed.get()) {
                            outputConsumer.accept("SERVER: Connection closed.");
                        }
                        break;
                    }
                    outputConsumer.accept(msgFromGroupChat);
                }
            } catch (IOException ignored) {
                // Socket closed locally or reset by peer
            } finally {
                closeEverything();
            }
        }, "chat-client-listener");
        listenerThread.setDaemon(true);
        listenerThread.start();
    }

    public void closeEverything() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closeQuietly(socket);
        closeQuietly(bufferedWriter);
        closeQuietly(bufferedReader);
    }

    @Override
    public void close() {
        closeEverything();
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

    private static String readLineRaw(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        while (true) {
            int b = in.read();
            if (b == -1) {
                return buffer.size() == 0 ? null : buffer.toString(StandardCharsets.UTF_8);
            }
            if (b == '\n') {
                byte[] bytes = buffer.toByteArray();
                int len = bytes.length;
                if (len > 0 && bytes[len - 1] == '\r') {
                    len--;
                }
                return new String(bytes, 0, len, StandardCharsets.UTF_8);
            }
            buffer.write(b);
        }
    }

    public static void main(String[] args) {
        String host = DEFAULT_HOST;
        int port = DEFAULT_PORT;

        if (args.length > 2) {
            System.err.println("Usage: java ChatClient [host] [port]");
            System.exit(1);
            return;
        }
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length == 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number: " + args[1]);
                System.exit(1);
                return;
            }
            if (port < 1 || port > 65535) {
                System.err.println("Port must be between 1 and 65535: " + port);
                System.exit(1);
                return;
            }
        }

        String username;
        try {
            System.out.print("Enter your username for the group chat: ");
            System.out.flush();
            username = readLineRaw(System.in);
        } catch (IOException e) {
            System.err.println("Failed to read username: " + e.getMessage());
            System.exit(1);
            return;
        }

        if (username == null || username.trim().isEmpty()) {
            System.err.println("Username cannot be empty.");
            System.exit(1);
            return;
        }

        try {
            Socket socket = new Socket(host, port);
            try (ChatClient client = new ChatClient(socket, username)) {
                client.listenForMessage();
                client.sendMessage();
            }
        } catch (IOException e) {
            System.err.println("Unable to connect to " + host + ":" + port + " - " + e.getMessage());
            System.exit(1);
        }
    }
}
