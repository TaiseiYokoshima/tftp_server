import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
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

    public static byte[] decode_short_to_unsigned_bytes(int num) {
        int unsigned16Bit = num & 0xFFFF;
        byte[] bytes = new byte[2];
        bytes[0] = (byte) ((unsigned16Bit >> 8) & 0xFF);
        bytes[1] = (byte) (unsigned16Bit & 0xFF);
        return bytes;
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
            }
            throw new RuntimeException(e);
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
                case 1: System.out.println("read initiated from " + packet.getAddress().toString().substring(1) + ":" + packet.getPort()); this.read(packet, buffer); break;
                case 2: System.out.println("write initiated from " + packet.getAddress().toString().substring(1) + ":" + packet.getPort()); this.write(packet, buffer); break;
            }

//            System.exit(0);
        }
    }

    public void read(DatagramPacket packet, byte[] buffer) throws Exception {
        InetAddress ip = packet.getAddress();
        int port = packet.getPort();
        String filepath = get_filepath(buffer);
        System.out.println("Sending file:  " + filepath);


        File f = new File(filepath);
        DatagramSocket session_socket = new DatagramSocket();


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

        int block_num = 1;
        boolean stay = true;
        while (stay) {
            System.out.println("_______________________________________________________");
            System.out.println("available bytes to read: " + inputStream.available());
            //exit condition
            //1 now there is no available bytes to be read
            //2 the available bytes is less than 512 which means this will be last packet
            // this will provide the last iteration of this loop

            int available = inputStream.available();
            if (available < 512) {
                System.out.println("hit last packet");
                stay = false;
            }

            int to_read = (stay) ? 512 : available;


            byte[] file_buffer = new byte[to_read];
            int result = inputStream.read(file_buffer, 0, to_read);



            DatagramPacket data_packet = this.generate_data_packet(block_num, ip, port, file_buffer);
            session_socket.send(data_packet);
            System.out.println("block " + block_num + " sent");



            int count = 1;
            while (true) {
                if (count > 10) {
                    System.err.println("Retransmitted too many times");
                    session_socket.close();
                    return;
                }
                byte[] ack_packet_buffer = new byte[4];
                DatagramPacket ack_packet = new DatagramPacket(ack_packet_buffer, ack_packet_buffer.length);
                try {
                    session_socket.receive(ack_packet);
                } catch (SocketTimeoutException e ) {
                    session_socket.send(data_packet);
                    System.out.println("block " + block_num + " retransmitted");
                    count++;
                    continue;
                }

                boolean check_result = this.check_packet_for_ack(ack_packet, ip, port, block_num);

                if (check_result) continue;


                System.out.println("ack received from " + ack_packet.getAddress().toString().substring(1) + ":" + ack_packet.getPort());
                break;
            }

        block_num++;
        }

        session_socket.close();
    }

    public String get_filepath(byte[] buffer) {
        String filepath = new String(Arrays.copyOfRange(buffer, 2, buffer.length), StandardCharsets.US_ASCII).split("\0")[0];

        if (filepath.startsWith("./")) return filepath.stripTrailing();
        if (filepath.startsWith(".\\")) return filepath.stripTrailing();

        //to do deal with \ in windows
        //deal with ./

        return "./" +  filepath;
    }

    public boolean check_packet_for_ack(DatagramPacket ack_packet, InetAddress ip, int port, int block_num) {
        byte[] ack_packet_buffer = ack_packet.getData();
        if (!ack_packet.getAddress().equals(ip) || ack_packet.getPort() != port) {
            System.out.println("stranger's packet");
            return true;
        }
        if (decode_code(ack_packet_buffer, false) != 4) {
            System.out.println("not an ack packet");
            return true;
        }
        if (decode_code(ack_packet_buffer, true) != block_num) {
            System.out.println("wrong block num");
            return true;
        }
        return false;
    }

    public Object[] check_packet_for_data(DatagramSocket session_socket, DatagramPacket packet, DatagramPacket last_packet, InetAddress ip, int port, int block_num, int count) throws Exception{
        if (!packet.getAddress().equals(ip) || packet.getPort() != port) {
            System.out.println("ip or port mismatch - packet dropped");
            return new Object[]{true, count};
        }
        if (decode_code(packet.getData(), false) != 3) {
            System.out.println("wrong opcode - packet dropped");
            return new Object[]{true, count};
        }
        if (decode_code(packet.getData(), true) != block_num) {
            System.out.printf("wrong block [%d] - packet dropped %n", decode_code(packet.getData(), true));
            session_socket.send(last_packet);
            return new Object[]{true, count + 1};
        }

        return new Object[]{false, count};
    }




    public void write(DatagramPacket packet, byte[] buffer) throws Exception {
        InetAddress ip = packet.getAddress();
        int port = packet.getPort();
        String filepath = get_filepath(buffer);
        System.out.println("Receiving file:  " + filepath);

        // timeout set high because when writing, we do not transmit data
        // the client retransmits the packet
        DatagramSocket session_socket = new DatagramSocket();
        session_socket.setSoTimeout(1000);

        // zero block ack
        DatagramPacket ack_packet = this.send_ack_packet(session_socket,0, ip, port);

        //stream to create file byte[]
        ByteArrayOutputStream stream = new ByteArrayOutputStream();

        int block_num = 1;
        int count = 1;
        boolean stay = true;
        while (stay) {
            if (count > 10) {
                System.err.println("Retransmitted too many times");
                session_socket.close();
                stream.close();
                return;
            }

            //packet for receiving data
            byte[] data_packet_buffer = new byte[516];
            DatagramPacket data_packet = new DatagramPacket(data_packet_buffer, data_packet_buffer.length);

            //attempts to receive data
            try {
                session_socket.receive(data_packet);
            // timeout error retransmits last ack packet
            } catch (SocketTimeoutException e) {
                System.err.println("Client disconnected without completing the request.");
                session_socket.close();
                return;
            }

            //checks the packet to see if it received the right packet
            Object[] check_result = this.check_packet_for_data(session_socket, data_packet, ack_packet, ip, port, block_num, count);
            count = (int) check_result[1];
            if ((boolean) check_result[0]) continue;



            //this point onwards means received the correct packet
            //resets retransmission count
            count = 1;

            //calculate the length of the data
            int length = data_packet.getLength() - 4;

            System.out.println("_______________________________________________________");
            String log = "block " + block_num + " received from " + data_packet.getAddress().toString().substring(1) + ":" + data_packet.getPort();
            System.out.println(log);


            // checks to see if this is the last block if so, sets the flag to exit the loop
            if (length < 512) {
                System.out.println("hit last block");
                stay = false;
            }

            System.out.println("data size: " + length);

            //writes the new data to the stream
            stream.write(data_packet_buffer, 4, length);

            //sends the ack packet
            ack_packet = this.send_ack_packet(session_socket,block_num, ip, port);
            System.out.println("ack sent");

            block_num++;
        }

        byte[] file_bytes = stream.toByteArray();

        FileOutputStream fos = new FileOutputStream(filepath);
        fos.write(file_bytes);
        fos.close();

        System.out.println("Write request completed.");
        session_socket.close();
    }

    public DatagramPacket generate_ack_packet(int block_num, InetAddress ip, int port) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(decode_short_to_unsigned_bytes(4));
        stream.write(decode_short_to_unsigned_bytes(block_num));
        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip,port);
    }

    public DatagramPacket generate_error_packet(InetAddress ip, int port) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(decode_short_to_unsigned_bytes(5));
        stream.write(decode_short_to_unsigned_bytes(1));
        stream.write("lmao\0".getBytes(StandardCharsets.US_ASCII));
        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip,port);
    }

    public DatagramPacket generate_data_packet(int block_num, InetAddress ip, int port, byte[] data) throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(decode_short_to_unsigned_bytes(3));
        stream.write(decode_short_to_unsigned_bytes(block_num));
        stream.write(data);
        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip,port);
    }

    public DatagramPacket send_ack_packet(DatagramSocket session_socket, int block_num, InetAddress ip, int port) throws Exception {
        DatagramPacket ack = this.generate_ack_packet(block_num, ip, port);
        session_socket.send(ack);
        return ack;
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
