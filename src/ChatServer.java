import java.io.IOException;
import java.util.*;
import java.net.*;

public class ChatServer {

    private ServerSocket serverSocket;

    public ChatServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    public void startServer() throws IOException {
        while(!serverSocket.isClosed()){
            Socket socket = serverSocket.accept();
            ClientHandler clientHandler= new ClientHandler(socket);
            Thread thread = new Thread(clientHandler);
            thread.start();
            System.out.println("A new client has connected!");
        }
    }
    public static void main(String[] args) throws IOException {
        ChatServer server = new ChatServer(1234);
        System.out.println("Server is running on port 1234...");
        server.startServer();
    }
}
