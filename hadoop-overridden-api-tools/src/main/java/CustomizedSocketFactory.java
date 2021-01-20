import org.apache.hadoop.net.StandardSocketFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CustomizedSocketFactory extends StandardSocketFactory {
    static List<Socket> SOCKETLIST = Collections.synchronizedList(new ArrayList<Socket>());

    @Override
    public Socket createSocket() throws IOException {
        Socket sock = super.createSocket();
        SOCKETLIST.add(sock);
        printStackTrace(sock);
        return sock;
    }

    private void printStackTrace(Socket socket) {
        try {
            Exception ex = new Exception("Cloudera Test Source for Socket " + socket.toString());
            throw ex;
        } catch (Exception ex) {
            //TBO ex.printStackTrace();
        }
    }

    @Override
    public Socket createSocket(InetAddress addr, int port) throws IOException {
        Socket sock = super.createSocket(addr, port);
        SOCKETLIST.add(sock);
        printStackTrace(sock);
        return sock;
    }

    @Override
    public Socket createSocket(InetAddress addr, int port, InetAddress localHostAddr, int localPort) throws IOException {
        Socket sock = super.createSocket(addr, port, localHostAddr, localPort);
        SOCKETLIST.add(sock);
        return sock;
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        Socket sock = super.createSocket(host, port);
        SOCKETLIST.add(sock);
        return sock;
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHostAddr, int localPort) throws IOException {
        Socket sock = super.createSocket(host, port, localHostAddr, localPort);
        SOCKETLIST.add(sock);
        return sock;
    }
}
