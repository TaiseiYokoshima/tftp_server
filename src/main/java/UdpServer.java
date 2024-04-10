import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.io.File;

public class UdpServer {
    private int port;
    private DatagramSocket socket;
    private byte[] file_or_mode = new byte[1000];
    private byte[] op_block_err = new byte[2];

    private byte[] packet_buffer = new byte[2000];


//    private final byte[] read_opcode = {0, 1};

    public static int decode_code(byte[] packet, boolean block_num) {
        if (block_num) {
            return packet[3];
        }
        int first = 0;
        int second = 1;



        //bitwise operation to appropriately parse unsigned 16 bit binary int to signed int
        int code = ((packet[first] & 0xFF) << 8) | (packet[second] & 0xFF);

        return code;
    }



    public static byte[] truncate(byte[] buffer) {
        int index = 0;
        for (int i = 0; i < buffer.length; i++) {
            if (buffer[i] == 0x00) {
                index = i;
                break;
            }
        }

        return Arrays.copyOfRange(buffer,0, index);
    }


    public UdpServer(int port)  throws SocketException, IllegalArgumentException, SecurityException {
        this.socket = new DatagramSocket(port);
    }

    public void start() throws Exception {
        System.out.println("Working Directory: " + System.getProperty("user.dir"));


        while (true) {
//            DatagramPacket packet = new DatagramPacket(op_block_err, op_block_err.length);


            byte[] buffer = new byte[2000];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            this.socket.receive(packet);

            System.out.println("received a packet");

            int code = decode_code(packet.getData(), false);

            switch (code) {
                case 1: System.out.println("read initiated"); this.read(packet, buffer); break;
                case 2: System.out.println("write initiated"); break;
            }

            System.exit(0);
        }
    }

    public void read(DatagramPacket packet, byte[] buffer) throws Exception {
        String packet_str = new String(Arrays.copyOfRange(buffer, 2, buffer.length), "US-ASCII");
        String[] splitted = packet_str.split("\0");

        String filepath = splitted[0];
        String mode = splitted[1];

        File f = new File(filepath);


        DatagramSocket session_socket = new DatagramSocket();
        InetAddress ip = packet.getAddress();
        int port = packet.getPort();

        if (!f.exists() || f.isDirectory()) {
            System.out.println("file not found or file is a directory");
            DatagramPacket err_packet = this.generate_error_packet(ip, port);
            session_socket.send(err_packet);
            return;
        }



        byte[] file_bytes = Files.readAllBytes(f.toPath());
        ByteArrayInputStream inputStream = new ByteArrayInputStream(file_bytes);

        session_socket.setSoTimeout(100);

        int i = 1;
        boolean stay = true;
        while (stay) {

            System.out.println("available bytes to read: " + inputStream.available());
            //exit condition
            //1 now there is no available bytes to be read
            //2 the available bytes is less than 512 which means this will be last packet
            if (inputStream.available() == 0 || inputStream.available() < 512) {
                System.out.println("hit last packet");
                stay = false;
            };



            byte[] file_buffer = new byte[512];
            inputStream.read(file_buffer, 0, 512);

            if (!stay) file_buffer = truncate(file_buffer);



            DatagramPacket data_packet = this.generate_data_packet(i, ip, port, file_buffer);
            session_socket.send(data_packet);
            System.out.println("block " + i + " sent");



            int count = 1;
            while (true) {
                if (count > 10) {
                    System.out.println("Retransmitted too many times");
                    return;
                }

                try {
                    byte[] ack_packet_buffer = new byte[4];
                    DatagramPacket ack_packet = new DatagramPacket(ack_packet_buffer, ack_packet_buffer.length);
                    session_socket.receive(ack_packet);
//                    if (ack_packet.getAddress() != ip) {
//                        System.out.println("stranger's packet");
//                        continue;
//                    }
                    if (decode_code(ack_packet_buffer, false) != 4) {
                        System.out.println("not an ack packet");
                        continue;
                    }
                    if (decode_code(ack_packet_buffer, true) != i) {
                        System.out.println("wrong block num");
                        continue;
                    }

                    System.out.println("ack received");

                    break;
                } catch (SocketTimeoutException e ) {
                    session_socket.send(data_packet);
                    System.out.println("block " + i + " retransmitted");
                }
                count++;
            }




        i++;
        }
    }

    public DatagramPacket generate_error_packet(InetAddress ip, int port) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(new byte[]{ (byte) 0, (byte) 5});
        stream.write(new byte[]{ (byte) 0, (byte) 1});
        stream.write("lmao\0".getBytes(StandardCharsets.US_ASCII));
        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip,port);
    }

    public DatagramPacket generate_data_packet(int block_num, InetAddress ip, int port, byte[] data) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(new byte[]{ (byte) 0, (byte) 3});
        stream.write(new byte[]{ (byte) 0, (byte) block_num});
        stream.write(data);
        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip,port);
    }

    public static void main(String[] args) throws Exception {
        int port = 69;
        UdpServer server = new UdpServer(port);
        System.out.println("running on " + server.socket.getLocalPort());
        server.start();
    }
}
