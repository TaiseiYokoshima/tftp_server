import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.io.File;

public class UdpServer {
    private final DatagramSocket socket;
    public static int decode_code(byte[] packet, boolean block_num) {
        int first;
        int second;

        if (block_num) {
            first = 2;
            second = 3;
        } else {
            first = 0;
            second = 1;
        }

        //bitwise operation to appropriately parse unsigned 16 bit binary int to signed int
        return ((packet[first] & 0xFF) << 8) | (packet[second] & 0xFF);
    }

    public static byte[] truncate_data(byte[] buffer) {
        //finds the index of the data buffer where the data ends
        int index = 0;
        for (int i = 0; i < buffer.length; i++) {
            if (buffer[i] == 0x00) {
                index = i;
                break;
            }
        }
        //copies and truncate the data with index
        return Arrays.copyOfRange(buffer,0, index);
    }


    public UdpServer(int port) throws IllegalArgumentException, SecurityException {
        try {
            this.socket = new DatagramSocket(port);
        } catch (java.net.BindException e) {
            String msg = e.getMessage();
            if (msg.equals("Permission denied")) {
                //this statement is let know that to use this port requires higher privilege
                // error just says
                if (port < 1024) {
                    System.err.println(msg +  ".\n You do not have permission to bind to this port: To run, elevate your privilege, choose another port, or randomize it by not providing the argument.");
                    System.exit(1);
                }

                throw new RuntimeException(e);
            } else {
                throw new RuntimeException(e);
            }
        } catch (java.net.SocketException socket_ex) {
            throw new RuntimeException(socket_ex);
        }
    }
    public UdpServer()  throws SocketException, IllegalArgumentException, SecurityException {
        this.socket = new DatagramSocket();
    }

    public void start() throws Exception {
        System.out.println("Working Directory: " + System.getProperty("user.dir"));
        while (true) {
            byte[] buffer = new byte[2000];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            this.socket.receive(packet);

            System.out.println("received a packet");

            int code = decode_code(packet.getData(), false);

            switch (code) {
                case 1: System.out.println("read initiated from " + packet.getAddress() + ":" + packet.getPort()); this.read(packet, buffer); break;
                case 2: System.out.println("write initiated"); break;
            }

            System.exit(0);
        }
    }

    public void read(DatagramPacket packet, byte[] buffer) throws Exception {
        String packet_str = new String(Arrays.copyOfRange(buffer, 2, buffer.length), "US-ASCII");
        String filepath = packet_str.split("\0")[0];
        File f = new File(filepath);

        DatagramSocket session_socket = new DatagramSocket();
        InetAddress ip = packet.getAddress();
        int port = packet.getPort();

        // error handling, file doesn't exist or filepath is a directory
        if (!f.exists() || f.isDirectory()) {
            System.out.println("file not found or file is a directory");
            DatagramPacket err_packet = this.generate_error_packet(ip, port);
            session_socket.send(err_packet);
            return;
        }

        //convert file to bytes
        byte[] file_bytes = Files.readAllBytes(f.toPath());

        //create a stream to read maximum of 512 bytes at a time
        ByteArrayInputStream inputStream = new ByteArrayInputStream(file_bytes);

        // set socket timeout so that after a given time the packet will be retransmitted
        session_socket.setSoTimeout(100);

        int i = 1;
        boolean stay = true;
        while (stay) {
            System.out.println("_______________________________________________________") ;
            System.out.println("available bytes to read: " + inputStream.available());
            //exit condition
            //1 now there is no available bytes to be read
            //2 the available bytes is less than 512 which means this will be last packet
            // this will provide the last iteration of this loop
            if (inputStream.available() == 0 || inputStream.available() < 512) {
                System.out.println("hit last packet");
                stay = false;
            };

            byte[] file_buffer = new byte[512];
            inputStream.read(file_buffer, 0, 512);

            // if this is the last packet, the data needs to be truncated
            if (!stay) file_buffer = truncate_data(file_buffer);
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
                    if (!ack_packet.getAddress().equals(ip) || ack_packet.getPort() != port) {
                        System.out.println("stranger's packet");
                        continue;
                    }

                    if (decode_code(ack_packet_buffer, false) != 4) {
                        System.out.println("not an ack packet");
                        continue;
                    }
                    if (decode_code(ack_packet_buffer, true) != i) {
                        System.out.println("wrong block num");
                        continue;
                    }

                    System.out.println("ack received from " + ack_packet.getAddress() + ":" + ack_packet.getPort());

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
        UdpServer server;
        if (args.length == 0) {
            server = new UdpServer();
            System.out.println("Note: port will be randomized because it wasn't specified");
        } else {
            int port = Integer.parseInt(args[0]);
            server = new UdpServer(port);
        }

        System.out.println("running on " + server.socket.getLocalPort());
        server.start();
    }
}
