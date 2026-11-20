# Java Multi-Client Chat

A lightweight, plain-Java multi-client TCP console chat application using `ServerSocket`, `Socket`, a bounded thread pool (`ExecutorService`), and UTF-8 newline-delimited framing.

## Architecture

- **`src/ChatServer.java`**: Listens on a configured TCP port (default `1234`), accepts incoming connections without blocking the `accept()` loop, manages an instance-scoped thread-safe set of connected handlers, and dispatches each client to a bounded `ThreadPoolExecutor` (max 128 concurrent connections). Implements `AutoCloseable` for clean shutdown.
- **`src/ClientHandler.java`**: Handles per-client UTF-8 I/O, performs a timeout-bounded (`30s`) username handshake inside `run()`, enforces username validation and uniqueness, applies server-side `username: message` framing (preventing spoofing), synchronizes per-recipient writes, isolates dead recipients on `IOException`, and cleans up idempotently on EOF or `/quit`.
- **`src/ChatClient.java`**: Connects to the chat server (`localhost:1234` by default), spawns a background listener thread for incoming broadcasts, reads user input from `stdin` (or an injected `InputStream` for testing), supports `/quit` and `/exit`, and exits cleanly on EOF or server disconnect.
- **`test/ChatAppTest.java`**: Self-contained pure-JDK integration test suite covering ephemeral server binding, UTF-8 broadcasts, EOF disconnect handling, dead-recipient isolation, Slowloris handshake resilience, username validation/spoofing prevention, multi-server isolation, and concurrent load.

## Prerequisites

- **JDK 21+** (tested on JDK 23 with `--release 21` compatibility)

## Build

```bash
javac -encoding UTF-8 -d out src/*.java
```

With strict lint warnings enabled:

```bash
javac -Xlint:all -Werror -encoding UTF-8 -d out src/*.java
```

## Run

1. Start the server (default port `1234`, or specify `[port]`):

```bash
java -cp out ChatServer
# or custom port:
java -cp out ChatServer 5555
```

2. Start one or more clients in separate terminals (default `localhost` `1234`, or specify `[host] [port]`):

```bash
java -cp out ChatClient
# or custom host/port:
java -cp out ChatClient localhost 5555
```

3. Enter a unique username (1–32 printable characters, excluding `SERVER`), type messages to broadcast, and type `/quit` or `/exit` (or send EOF) to disconnect cleanly.

## Wire Protocol

- Transport: TCP stream, UTF-8 encoded, newline-delimited (`\n` or `\r\n`).
- Handshake: The client sends its desired username on the first line (must be 1–32 printable characters, unique across active clients, and not `SERVER`).
- Chat messages: Each subsequent line sent by an authenticated client is sanitized of ISO control characters and broadcast to all other active clients prefixed server-side as `<username>: <message>`.
- Server notices: System notifications (join, leave, validation errors) are prefixed with `SERVER: `.
- Disconnect: Sending `/quit`, `/exit`, or closing the output stream (EOF) cleanly removes the client and notifies remaining peers.

## Run Tests

Compile both `src/` and `test/` and run `ChatAppTest`:

```bash
javac -Xlint:all -Werror -encoding UTF-8 -d out src/*.java test/*.java
java -cp out ChatAppTest
```
