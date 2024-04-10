import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
public class client {
    public static void main(String[] args ) throws Exception{

        DatagramSocket socket = new DatagramSocket(12346);

        InetAddress ip = InetAddress.getByName("127.0.0.1");
        int port = 12345;
        String message = "hello";

        byte[] messbyte = message.getBytes();


        DatagramPacket packet = new DatagramPacket(messbyte, messbyte.length, ip, port);

        socket.send(packet);
        System.out.println("packet sent");



        socket.close();
        System.out.println("socket closed");

    }
}
