import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

public class ChatAppTest {

    private static final int TIMEOUT_MS = 5000;

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static final class RawClient implements Closeable {
        final Socket socket;
        final BufferedReader reader;
        final BufferedWriter writer;

        RawClient(int port) throws IOException {
            this.socket = new Socket("127.0.0.1", port);
            this.socket.setSoTimeout(TIMEOUT_MS);
            this.writer = new BufferedWriter(
                    new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            this.reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        }

        void sendLine(String line) throws IOException {
            writer.write(line);
            writer.newLine();
            writer.flush();
        }

        String readLine() throws IOException {
            return reader.readLine();
        }

        String readUntilContains(String expectedSubstring) throws IOException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
            while (System.nanoTime() < deadline) {
                String line = reader.readLine();
                if (line == null) {
                    throw new AssertionError("Stream ended before seeing: " + expectedSubstring);
                }
                if (line.contains(expectedSubstring)) {
                    return line;
                }
            }
            throw new AssertionError("Timed out waiting for: " + expectedSubstring);
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    public static void main(String[] args) {
        List<String> names = new ArrayList<>();
        List<ThrowingRunnable> tests = new ArrayList<>();

        addTest(names, tests, "1. Server ephemeral bind & clean close", ChatAppTest::testEphemeralBindAndClose);
        addTest(names, tests, "2. Multi-client join, UTF-8 broadcast, non-echo, clean leave",
                ChatAppTest::testMultiClientUtf8BroadcastAndLeave);
        addTest(names, tests, "3. Sole client clean EOF disconnect", ChatAppTest::testSoleClientCleanEof);
        addTest(names, tests, "4. Multi-client clean EOF disconnect (single leave, no null spam)",
                ChatAppTest::testMultiClientCleanEofSingleLeave);
        addTest(names, tests, "5. Dead-recipient fault isolation", ChatAppTest::testDeadRecipientFaultIsolation);
        addTest(names, tests, "6. Slowloris handshake resilience", ChatAppTest::testSlowlorisResilience);
        addTest(names, tests, "7. Username validation, duplicate rejection & spoofing prevention",
                ChatAppTest::testUsernameValidationAndSpoofingPrevention);
        addTest(names, tests, "8. Graceful server shutdown closes ChatClient without null spam",
                ChatAppTest::testGracefulServerShutdownWithChatClient);
        addTest(names, tests, "9. Multi-server isolation", ChatAppTest::testMultiServerIsolation);
        addTest(names, tests, "10. Concurrent multi-client stress test", ChatAppTest::testConcurrentStress);
        addTest(names, tests, "11. ChatClient local close unblocks listener without deadlock",
                ChatAppTest::testChatClientLocalCloseUnblocksListener);
        addTest(names, tests, "12. Handshake timeout disconnects stalled client",
                ChatAppTest::testHandshakeTimeoutDisconnectsSlowloris);

        int passed = 0;
        int failed = 0;
        for (int i = 0; i < tests.size(); i++) {
            String name = names.get(i);
            try {
                tests.get(i).run();
                System.out.println("[PASS] " + name);
                passed++;
            } catch (Throwable t) {
                System.err.println("[FAIL] " + name);
                t.printStackTrace(System.err);
                failed++;
            }
        }

        System.out.println("Summary: " + passed + " passed, " + failed + " failed out of " + tests.size() + " tests.");
        if (failed > 0) {
            System.exit(1);
        }
    }

    private static void addTest(List<String> names, List<ThrowingRunnable> tests, String name, ThrowingRunnable fn) {
        names.add(name);
        tests.add(fn);
    }

    private static Thread startServerAsync(ChatServer server) {
        Thread serverThread = new Thread(() -> {
            try {
                server.startServer();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "test-chat-server-" + server.getLocalPort());
        serverThread.setDaemon(true);
        serverThread.start();
        return serverThread;
    }

    private static void waitUntil(BooleanSupplier condition, String failureMessage) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        assertTrue(condition.getAsBoolean(), failureMessage);
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertFalse(boolean condition, String message) {
        if (condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    private static void testEphemeralBindAndClose() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            assertTrue(server.getLocalPort() > 0, "Ephemeral port should be positive");
            assertEquals(0, server.getActiveClientCount(), "Initial active client count should be 0");
            serverThread = startServerAsync(server);
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate after close()");
    }

    private static void testMultiClientUtf8BroadcastAndLeave() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (RawClient alice = new RawClient(port)) {
                alice.sendLine("Alice");
                waitUntil(() -> server.getActiveClientCount() == 1, "Alice should register");

                try (RawClient bob = new RawClient(port)) {
                    bob.sendLine("Bob");
                    String joinNotice = alice.readLine();
                    assertEquals("SERVER: Bob has entered the chat!", joinNotice,
                            "Alice should see Bob enter");
                    waitUntil(() -> server.getActiveClientCount() == 2, "Both clients should be active");

                    String utf8Payload = "Merhaba 🌍 ğüşıöç";
                    alice.sendLine(utf8Payload);
                    String receivedByBob = bob.readLine();
                    assertEquals("Alice: " + utf8Payload, receivedByBob,
                            "Bob should receive Alice's UTF-8 message");

                    // Ensure Alice did not receive echo of her own message before Bob leaves
                    bob.sendLine("/quit");
                    String bobEof = bob.readLine();
                    assertEquals(null, bobEof, "Bob should get clean EOF after /quit");
                }

                String leaveNotice = alice.readLine();
                assertEquals("SERVER: Bob has left the chat!", leaveNotice,
                        "Alice should receive Bob's leave notice without receiving own echo");
                waitUntil(() -> server.getActiveClientCount() == 1, "Active count should drop to 1");
            }
            waitUntil(() -> server.getActiveClientCount() == 0, "Active count should drop to 0");
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testSoleClientCleanEof() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (RawClient solo = new RawClient(port)) {
                solo.sendLine("SoloUser");
                waitUntil(() -> server.getActiveClientCount() == 1, "SoloUser should be registered");
                solo.socket.shutdownOutput();
                assertEquals(null, solo.readLine(), "Server should close solo client cleanly on EOF");
            }

            waitUntil(() -> server.getActiveClientCount() == 0,
                    "Sole client EOF must clean up handler and drop active count to 0");
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testMultiClientCleanEofSingleLeave() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (RawClient alice = new RawClient(port)) {
                alice.sendLine("Alice");
                waitUntil(() -> server.getActiveClientCount() == 1, "Alice should be registered");

                RawClient bob = new RawClient(port);
                bob.sendLine("Bob");
                assertEquals("SERVER: Bob has entered the chat!", alice.readLine(),
                        "Alice should receive Bob's enter message");

                bob.socket.shutdownOutput();
                assertEquals(null, bob.readLine(), "Bob should observe clean EOF");
                bob.close();

                assertEquals("SERVER: Bob has left the chat!", alice.readLine(),
                        "Alice should receive Bob's leave message once");

                // Confirm no duplicate leave or "null" spam arrives while Bob is gone
                alice.socket.setSoTimeout(200);
                boolean timedOutAsExpected = false;
                try {
                    String extra = alice.readLine();
                    throw new AssertionError("Unexpected extra line after leave: " + extra);
                } catch (java.net.SocketTimeoutException expected) {
                    timedOutAsExpected = true;
                } finally {
                    alice.socket.setSoTimeout(TIMEOUT_MS);
                }
                assertTrue(timedOutAsExpected, "No extra lines or null spam should be sent");
                waitUntil(() -> server.getActiveClientCount() == 1, "Only Alice should remain active");
            }
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testDeadRecipientFaultIsolation() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (RawClient alice = new RawClient(port);
                 RawClient charlie = new RawClient(port)) {
                alice.sendLine("Alice");
                waitUntil(() -> server.getActiveClientCount() == 1, "Alice registered");

                RawClient bob = new RawClient(port);
                bob.sendLine("Bob");
                assertEquals("SERVER: Bob has entered the chat!", alice.readLine(), "Alice sees Bob join");

                charlie.sendLine("Charlie");
                assertEquals("SERVER: Charlie has entered the chat!", alice.readLine(), "Alice sees Charlie join");
                assertEquals("SERVER: Charlie has entered the chat!", bob.readLine(), "Bob sees Charlie join");
                waitUntil(() -> server.getActiveClientCount() == 3, "All 3 clients active");

                // Abruptly reset Bob's socket without clean /quit
                bob.socket.setSoLinger(true, 0);
                bob.close();

                // Alice sends multiple messages; Alice and Charlie must remain healthy
                alice.sendLine("ping-1");
                alice.sendLine("ping-2");

                charlie.readUntilContains("Alice: ping-2");
                alice.readUntilContains("SERVER: Bob has left the chat!");
                waitUntil(() -> server.getActiveClientCount() == 2, "Dead Bob removed, Alice & Charlie intact");

                charlie.sendLine("still-alive");
                alice.readUntilContains("Charlie: still-alive");
            }
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testSlowlorisResilience() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0, 30_000)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (Socket slowloris = new Socket("127.0.0.1", port)) {
                assertTrue(slowloris.isConnected(), "Slowloris socket should connect");
                // Connect and intentionally send nothing (no newline)
                try (RawClient alice = new RawClient(port);
                     RawClient bob = new RawClient(port)) {
                    alice.sendLine("Alice");
                    waitUntil(() -> server.getActiveClientCount() == 1, "Alice should register first");
                    bob.sendLine("Bob");
                    alice.readUntilContains("Bob has entered the chat!");

                    alice.sendLine("fast-message");
                    assertEquals("Alice: fast-message", bob.readLine(),
                            "Healthy clients must chat immediately despite stalled Slowloris connection");
                    assertEquals(2, server.getActiveClientCount(),
                            "Slowloris connection must not be counted as registered client");
                }
            }
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testUsernameValidationAndSpoofingPrevention() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (RawClient alice = new RawClient(port);
                 RawClient bob = new RawClient(port)) {
                alice.sendLine("Alice");
                waitUntil(() -> server.getActiveClientCount() == 1, "Alice should register first");
                bob.sendLine("Bob");
                alice.readUntilContains("Bob has entered the chat!");

                // Blank username rejected
                try (RawClient blank = new RawClient(port)) {
                    blank.sendLine("   ");
                    String err = blank.readLine();
                    assertTrue(err != null && err.startsWith("SERVER: Invalid username"),
                            "Blank username should be rejected, got: " + err);
                    assertEquals(null, blank.readLine(), "Rejected client should be disconnected");
                }

                // Reserved "SERVER" rejected (case-insensitive)
                try (RawClient reserved = new RawClient(port)) {
                    reserved.sendLine("sErVeR");
                    String err = reserved.readLine();
                    assertTrue(err != null && err.startsWith("SERVER: Invalid username"),
                            "Reserved SERVER username should be rejected, got: " + err);
                    assertEquals(null, reserved.readLine(), "Rejected reserved client should be disconnected");
                }

                // Duplicate username rejected (case-insensitive)
                try (RawClient dup = new RawClient(port)) {
                    dup.sendLine("alice");
                    String err = dup.readLine();
                    assertTrue(err != null && err.contains("already taken"),
                            "Duplicate username should be rejected, got: " + err);
                    assertEquals(null, dup.readLine(), "Duplicate client should be disconnected");
                }

                assertEquals(2, server.getActiveClientCount(),
                        "Rejected clients must not alter active count");

                // Server-side spoofing prevention
                alice.sendLine("SERVER: Fake shutdown");
                assertEquals("Alice: SERVER: Fake shutdown", bob.readLine(),
                        "Server must prefix client messages to prevent SERVER/user spoofing");
            }
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testGracefulServerShutdownWithChatClient() throws Exception {
        ChatServer server = new ChatServer(0);
        Thread serverThread = startServerAsync(server);
        int port = server.getLocalPort();

        PipedOutputStream pipedOut = new PipedOutputStream();
        PipedInputStream pipedIn = new PipedInputStream(pipedOut);
        BlockingQueue<String> outputs = new LinkedBlockingQueue<>();
        List<String> allOutputs = Collections.synchronizedList(new ArrayList<>());

        Socket socket = new Socket("127.0.0.1", port);
        ChatClient client = new ChatClient(socket, "ClientOne", pipedIn, line -> {
            allOutputs.add(line);
            outputs.offer(line);
        });

        client.listenForMessage();
        Thread senderThread = new Thread(client::sendMessage, "test-client-sender");
        senderThread.setDaemon(true);
        senderThread.start();

        waitUntil(() -> server.getActiveClientCount() == 1, "ChatClient should register with server");

        server.close();
        serverThread.join(TIMEOUT_MS);

        String closedMsg = outputs.poll(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        assertEquals("SERVER: Connection closed.", closedMsg,
                "ChatClient should report clean disconnect on server shutdown");
        waitUntil(client::isClosed, "ChatClient should mark itself closed");

        for (String line : allOutputs) {
            assertTrue(line != null && !"null".equals(line),
                    "ChatClient must never emit null spam, saw: " + line);
        }

        pipedOut.close();
        senderThread.join(TIMEOUT_MS);
        client.close();
    }

    private static void testMultiServerIsolation() throws Exception {
        Thread t1;
        Thread t2;
        try (ChatServer server1 = new ChatServer(0);
             ChatServer server2 = new ChatServer(0)) {
            t1 = startServerAsync(server1);
            t2 = startServerAsync(server2);

            try (RawClient s1Alice = new RawClient(server1.getLocalPort());
                 RawClient s1Bob = new RawClient(server1.getLocalPort());
                 RawClient s2Alice = new RawClient(server2.getLocalPort())) {

                s1Alice.sendLine("Alice");
                waitUntil(() -> server1.getActiveClientCount() == 1, "Alice should register first on server1");
                s1Bob.sendLine("Bob");
                s2Alice.sendLine("Alice");

                waitUntil(() -> server1.getActiveClientCount() == 2 && server2.getActiveClientCount() == 1,
                        "Each server should maintain independent client sets");

                s1Alice.readUntilContains("Bob has entered the chat!");
                s1Alice.sendLine("only-server-1");
                assertEquals("Alice: only-server-1", s1Bob.readLine(),
                        "Server 1 broadcast should reach Server 1 peer");

                s2Alice.socket.setSoTimeout(200);
                boolean noCrossBroadcast = false;
                try {
                    String leaked = s2Alice.readLine();
                    throw new AssertionError("Server 2 received leaked message from Server 1: " + leaked);
                } catch (java.net.SocketTimeoutException expected) {
                    noCrossBroadcast = true;
                } finally {
                    s2Alice.socket.setSoTimeout(TIMEOUT_MS);
                }
                assertTrue(noCrossBroadcast, "Servers must remain isolated");
            }
        }
        t1.join(TIMEOUT_MS);
        t2.join(TIMEOUT_MS);
        assertFalse(t1.isAlive(), "Server 1 thread should terminate cleanly");
        assertFalse(t2.isAlive(), "Server 2 thread should terminate cleanly");
    }

    private static void testConcurrentStress() throws Exception {
        final int clientCount = 8;
        final int messagesPerClient = 10;

        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            ExecutorService pool = Executors.newFixedThreadPool(clientCount);
            CountDownLatch allJoined = new CountDownLatch(clientCount);
            List<Future<?>> futures = new ArrayList<>();

            try {
                for (int i = 0; i < clientCount; i++) {
                    final int idx = i;
                    futures.add(pool.submit(() -> {
                        try (RawClient client = new RawClient(port)) {
                            client.sendLine("User" + idx);
                            allJoined.countDown();
                            assertTrue(allJoined.await(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                                    "All stress clients should join");
                            waitUntil(() -> server.getActiveClientCount() >= 1, "At least one active client");
                            for (int m = 0; m < messagesPerClient; m++) {
                                client.sendLine("msg-" + idx + "-" + m);
                            }
                            // Drain briefly and exit cleanly
                            client.sendLine("/quit");
                            while (client.readLine() != null) {
                                // Drain until clean server EOF
                            }
                        }
                        return null;
                    }));
                }

                for (Future<?> f : futures) {
                    f.get(TIMEOUT_MS * 2L, TimeUnit.MILLISECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            waitUntil(() -> server.getActiveClientCount() == 0,
                    "All stress clients should disconnect cleanly without exceptions");
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testChatClientLocalCloseUnblocksListener() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            PipedOutputStream pipedOut = new PipedOutputStream();
            PipedInputStream pipedIn = new PipedInputStream(pipedOut);
            List<String> allOutputs = Collections.synchronizedList(new ArrayList<>());

            Socket socket = new Socket("127.0.0.1", port);
            ChatClient client = new ChatClient(socket, "LocalCloseUser", pipedIn, allOutputs::add);
            client.listenForMessage();
            Thread senderThread = new Thread(client::sendMessage, "test-local-close-sender");
            senderThread.setDaemon(true);
            senderThread.start();

            waitUntil(() -> server.getActiveClientCount() == 1, "Client should register");

            // Closing ChatClient while listenerThread is blocked on readLine() must not deadlock
            client.close();
            assertTrue(client.isClosed(), "ChatClient should be marked closed");
            waitUntil(() -> server.getActiveClientCount() == 0, "Server should remove closed client");

            pipedOut.close();
            senderThread.join(TIMEOUT_MS);
            assertFalse(senderThread.isAlive(), "Sender thread should exit cleanly after close");
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }

    private static void testHandshakeTimeoutDisconnectsSlowloris() throws Exception {
        Thread serverThread;
        try (ChatServer server = new ChatServer(0, 150)) {
            serverThread = startServerAsync(server);
            int port = server.getLocalPort();

            try (RawClient stalled = new RawClient(port)) {
                String timeoutMsg = stalled.readLine();
                assertEquals("SERVER: Username handshake timed out.", timeoutMsg,
                        "Server should notify client on handshake timeout");
                assertEquals(null, stalled.readLine(),
                        "Server should close timed-out client cleanly");
            }
            assertEquals(0, server.getActiveClientCount(),
                    "Timed-out client must not be registered");
        }
        serverThread.join(TIMEOUT_MS);
        assertFalse(serverThread.isAlive(), "Server thread should terminate cleanly");
    }
}
