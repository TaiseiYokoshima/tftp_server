import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UdpServer {
    private final DatagramSocket socket;
    private static final ConcurrentHashMap<String, Entry<Integer, ReentrantReadWriteLock>> file_map =  new ConcurrentHashMap<>();
    private static ReentrantLock file_map_lock = new ReentrantLock();

    public static void main(String[] args) throws Exception {
        UdpServer server;
        if (args.length == 0) {
            server = new UdpServer();
            System.out.println("Note: port will be randomized because it wasn't specified");
        } else {
            int port = Integer.parseInt(args[0]);
            server = new UdpServer(port);
        }

        System.out.printf("Accepting connections on %s : %d\n", server.socket.getLocalAddress().toString().substring(1), server.socket.getLocalPort());
        server.start();
    }

    private static boolean validate_write_path(String filepath) {
        int index = filepath.lastIndexOf('/');
        if (index < 0) return true;
        String directory_path = filepath.substring(0, index);
        File directory = new File(directory_path);
        return directory.exists() && directory.isDirectory();
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
                    System.err.println(msg +  "\n You do not have permission to bind to this port: To run, elevate your privilege, choose another port, or randomize it by not providing the argument.");
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

    @SuppressWarnings("InfiniteLoopStatement")
    public void start() {
        System.out.println("Working Directory: " + System.getProperty("user.dir"));
        while (true) {
            try {
                byte[] buffer = new byte[2000];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                this.socket.receive(packet);
                int code = decode_code(packet.getData(), false);
                String ip_str = packet.getAddress().toString().substring(1);
                int port = packet.getPort();
                switch (code) {
                    case 1:
                        System.out.println("Read initiated from " + ip_str + ":" + port);
                        Read read_session = new Read(packet, buffer, file_map_lock);
                        read_session.start();
                        break;
                    case 2:
                        System.out.println("Write initiated from " + ip_str + ":" + port);
                        Write write_session = new Write(packet, buffer, file_map_lock);
                        write_session.start();
                        break;
                }
            } catch (IOException e) {
                System.err.println("Error occurred when accepting request packet | " + e.getMessage());
            }
        }
    }





    private static int decode_code(byte[] packet, boolean block_num) {
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

    private static byte[] decode_short_to_unsigned_bytes(int num) {
        int unsigned16Bit = num & 0xFFFF;
        byte[] bytes = new byte[2];
        bytes[0] = (byte) ((unsigned16Bit >> 8) & 0xFF);
        bytes[1] = (byte) (unsigned16Bit & 0xFF);
        return bytes;
    }

    private static String get_filepath(byte[] buffer) {
        String filepath = new String(Arrays.copyOfRange(buffer, 2, buffer.length), StandardCharsets.US_ASCII).split("\0")[0];

        if (filepath.startsWith("./")) return filepath.stripTrailing();
        if (filepath.startsWith(".\\")) return filepath.stripTrailing();

        //to do deal with \ in windows
        //deal with ./

        return "./" +  filepath;
    }

    private static void close_all_streams(String ip, int port, Closeable... resources ) {
        for(Closeable resource : resources) {
            try {
                resource.close();
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not close resource due to IOException | %s\n", ip,  port, e.getMessage());
            }
        }
    }

    private static String format_filepath(String filepath) {
        String output = filepath;
        output = output.replace('\\', '/');
        if (output.startsWith("./")) output = output.substring(2);
        return output;
    }





    public static class Read extends Thread {
        DatagramPacket packet;
        byte[] buffer;
        ReentrantLock map_lock;
        ReentrantReadWriteLock file_lock;

        public Read(DatagramPacket packet, byte[] buffer, ReentrantLock file_map_lock) {
            this.packet = packet;
            this.buffer = buffer;
            this.map_lock = file_map_lock;
        }

        public void run() {
            InetAddress ip = packet.getAddress();
            String ip_str = ip.toString().substring(1);
            int port = packet.getPort();
            String filepath = get_filepath(buffer);
            filepath = format_filepath(filepath);

            File f = new File(filepath);
            DatagramSocket session_socket;
            try {
                session_socket = new DatagramSocket();
                session_socket.setSoTimeout(100);
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not create session socket and set time out | Terminating session | %s\n", ip_str, port, e.getMessage());
                return;
            }

            // error handling, file doesn't exist or filepath is a directory
            if (!f.exists() || f.isDirectory()) {
                System.err.printf("Client Session: %s %d | Read request denied due to either file being absent or a directory : %s\n", ip_str, port, filepath);
                send_err_packet(session_socket, ip, port, "File not found");
                session_socket.close();
                return;
            }





            map_lock.lock();
            try {
                Entry<Integer, ReentrantReadWriteLock> entry;
                if (file_map.containsKey(filepath)) {
                    entry = file_map.get(filepath);
                    file_lock = entry.getValue();
                    entry = new AbstractMap.SimpleImmutableEntry<>(entry.getKey() + 1, file_lock);
                } else {
                    file_lock = new ReentrantReadWriteLock();
                    entry = new AbstractMap.SimpleImmutableEntry<>(1, file_lock);
                }

                file_map.put(filepath, entry);
                System.out.printf("Client Session: %s %d | Acquired lock and wrote to file map\n", ip_str, port);
            } catch (Exception e) {
                System.err.printf("Client Session: %s %d | Could not acquire lock and write to file map | Terminating session | %s\n", ip_str, port, e.getMessage());
                session_socket.close();
                return;
            } finally {
                map_lock.unlock();
            }



            //convert file to bytes
            byte[] file_bytes;


            file_lock.readLock().lock();
            try {
                file_bytes = Files.readAllBytes(f.toPath());
                System.out.printf("Client Session: %s %d | Acquired file lock and read file\n", ip_str, port);
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Failed to acquire file lock and read from file | Terminating session | %s\n", ip_str, port, e.getMessage());
                session_socket.close();
                return;
            } finally {
                file_lock.readLock().unlock();
            }


            System.out.printf("Client Session: %s %d | Sending file: %s ( %d bytes) \n", ip_str, port, filepath, file_bytes.length);

            //create a stream to read maximum of 512 bytes at a time
            ByteArrayInputStream inputStream = new ByteArrayInputStream(file_bytes);

            DatagramPacket data_packet;

            int block_num = 1;
            boolean stay = true;
            while (stay) {

                Object[] data_sent_check = send_data_packet(session_socket, inputStream, ip, port, block_num);
                if (data_sent_check == null) {
                    close_all_streams(ip_str, port, session_socket, inputStream);
                    return;
                }

                data_packet = (DatagramPacket) data_sent_check[0];

                stay = (boolean) data_sent_check[1];

                boolean ack_check = accept_ack_packet(session_socket, data_packet, ip, port, block_num);

                if (ack_check) continue;

                block_num++;
            }



            close_all_streams(ip_str, port, session_socket, inputStream);
            session_socket.close();

            map_lock.lock();
            try {
                Entry<Integer, ReentrantReadWriteLock> entry = file_map.get(filepath);
                int num_of_access = entry.getKey();

                if (num_of_access < 1) file_map.remove(filepath);
                else {
                    entry = new AbstractMap.SimpleImmutableEntry<>(num_of_access - 1, file_lock);
                    file_map.put(filepath, entry);
                }

                System.out.printf("Client Session: %s %d | Acquired lock and wrote to file map\n", ip_str, port);
            } catch (Exception e) {
                System.err.printf("Client Session: %s %d | Failed to acquire lock and read from file map | %s\n", ip_str, port, e.getMessage());
                session_socket.close();
                return;
            } finally {
                map_lock.unlock();
            }

            System.out.printf("Client Session: %s %d | Read request completed\n", ip_str, port);
        }
    }



    private static boolean check_packet_for_ack(DatagramPacket ack_packet, InetAddress ip, int port, int block_num) {
        String ip_str = ip.toString().substring(1);
        byte[] ack_packet_buffer = ack_packet.getData();
        if (!ack_packet.getAddress().equals(ip) || ack_packet.getPort() != port) {
            System.out.printf("Client Session: %s %d | Ip or port mismatch | Packet dropped\n", ip_str,  port);
            return true;
        }
        if (decode_code(ack_packet_buffer, false) != 4) {
            System.out.printf("Client Session: %s %d | Code mismatch | Packet dropped\n", ip_str,  port);
            return true;
        }
        if (decode_code(ack_packet_buffer, true) != block_num) {
            System.out.printf("Client Session: %s %d | Block mismatch | Packet dropped\n", ip_str,  port);
            return true;
        }
        return false;
    }

    private static DatagramPacket generate_error_packet(InetAddress ip, int port, String message) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            stream.write(decode_short_to_unsigned_bytes(5));
            stream.write(decode_short_to_unsigned_bytes(1));
            stream.write((message + "\0").getBytes(StandardCharsets.US_ASCII));
        } catch (IOException e) {
            System.err.printf("Client Session: %s %d | Could not generate error packet due to IOException | Terminating session | %s\n", ip,  port, e.getMessage());
            return null;
        }
        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip, port);
    }

    private static DatagramPacket generate_data_packet(int block_num, InetAddress ip, int port, byte[] data) {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            stream.write(decode_short_to_unsigned_bytes(3));
            stream.write(decode_short_to_unsigned_bytes(block_num));
            stream.write(data);
            byte[] packet_data = stream.toByteArray();
            return new DatagramPacket(packet_data, packet_data.length, ip, port);
        } catch (IOException e) {
            System.err.printf("Client Session: %s %d | Could not generate data packet due to IOException | Terminating session | %s\n", ip.toString().substring(1),  port, e.getMessage());
            return null;
        }
    }

    private static void send_err_packet(DatagramSocket session_socket, InetAddress ip, int port, String message) {
        DatagramPacket errorPacket = generate_error_packet(ip, port, message);
        if (errorPacket == null) return;
        try {
            session_socket.send(errorPacket);
        } catch (IOException e) {
            System.err.printf("Client Session: %s %d | Could not send error packet due to IOException | %s\n", ip,  port, e.getMessage());
        }
    }

    @SuppressWarnings("unused")
    private static Object[] send_data_packet(DatagramSocket session_socket, InputStream inputStream, InetAddress ip, int port, int block_num) {
        String ip_str = ip.toString().substring(1);
        try {
            boolean stay = true;
            int available = inputStream.available();

            System.out.printf("Client Session: %s %d | Available bytes to read: %d \n", ip_str, port, available);
            if (available < 512) {
                System.out.printf("Client Session: %s %d | Hit last block \n", ip_str, port);
                stay = false;
            }

            int to_read = (stay) ? 512 : available;


            byte[] file_buffer = new byte[to_read];
            int result = inputStream.read(file_buffer, 0, to_read);
            DatagramPacket data_packet = generate_data_packet(block_num, ip, port, file_buffer);

            if (data_packet == null) return null;

            session_socket.send(data_packet);
            System.out.printf("Client Session: %s %d | Block %d sent\n", ip_str, port, block_num);

            return new Object[]{data_packet, stay};
        } catch (IOException e) {
            System.err.printf("Client Session: %s %d | Could not send data packet due to IOException | Terminating session | %s\n", ip_str,  port, e.getMessage());
            return null;
        }
    }

    private static boolean accept_ack_packet(DatagramSocket session_socket, DatagramPacket data_packet, InetAddress ip, int port, int block_num) {
        String ip_str = ip.toString().substring(1);
        int count = 0;
        while (true) {
            if (count > 10) {
                System.err.printf("Client Session: %s %d | Client timed out | Terminating session\n", ip_str,  port);
                session_socket.close();
                return true;
            }
            byte[] ack_packet_buffer = new byte[4];
            DatagramPacket ack_packet = new DatagramPacket(ack_packet_buffer, ack_packet_buffer.length);
            try {
                session_socket.receive(ack_packet);
            } catch (SocketTimeoutException e) {
                try {
                    session_socket.send(data_packet);
                } catch (IOException ioe) {
                    System.err.printf("Client Session: %s %d | Could not retransmit data packet due to IOException | Terminating session | %s\n", ip_str,  port, ioe.getMessage());
                    return true;
                }
                count++;
                continue;
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not receive ack packet due to IOException | Terminating session | %s\n", ip_str,  port, e.getMessage());
                return true;
            }


            boolean check_result = check_packet_for_ack(ack_packet, ip, port, block_num);

            if (check_result) continue;

            System.out.printf("Client Session: %s %d | Ack %d received\n", ip_str,  port, block_num);
            return false;
        }
    }





    public static class Write extends Thread {
        DatagramPacket packet;
        byte[] buffer;
        ReentrantLock map_lock;
        ReentrantReadWriteLock file_lock;
        public Write(DatagramPacket packet, byte[] buffer, ReentrantLock file_map_lock) {
            this.packet = packet;
            this.buffer = buffer;
            this.map_lock = file_map_lock;
        }

        public void run() {
            InetAddress ip = packet.getAddress();
            String ip_str = ip.toString().substring(1);
            int port = packet.getPort();

            String filepath = get_filepath(buffer);
            filepath = format_filepath(filepath);


            System.out.printf("Client Session: %s %d | Receiving file : %s\n", ip_str, port, filepath);

            // timeout set high because when writing, we do not transmit data
            // the client retransmits the packet
            DatagramSocket session_socket;
            try {
                session_socket = new DatagramSocket();
                session_socket.setSoTimeout(1000);
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not create socket or set time out due to IOException | Terminating session | %s\n", ip_str, port, e.getMessage());
                return;
            }

            if (!validate_write_path(filepath)) {
                send_err_packet(session_socket, ip, port,"Directory not found");
                System.err.printf("Client Session: %s %d | Write request denied due to absent directory in given filepath\n", ip_str, port);
                session_socket.close();
                return;
            }


            map_lock.lock();
            try {
                Entry<Integer, ReentrantReadWriteLock> entry;
                if (file_map.containsKey(filepath)) {
                    entry = file_map.get(filepath);
                    file_lock = entry.getValue();
                    entry = new AbstractMap.SimpleImmutableEntry<>(entry.getKey() + 1, file_lock);
                } else {
                    file_lock = new ReentrantReadWriteLock();
                    entry = new AbstractMap.SimpleImmutableEntry<>(1, file_lock);
                }

                file_map.put(filepath, entry);
                System.out.printf("Client Session: %s %d | Acquired lock and wrote to file map\n", ip_str, port);
            } catch (Exception e) {
                System.err.printf("Client Session: %s %d | Could not acquire lock and write to file map | Terminating session | %s\n", ip_str, port, e.getMessage());
                session_socket.close();
                return;
            } finally {
                map_lock.unlock();
            }




            // zero block ack
            DatagramPacket ack_packet = send_ack_packet(session_socket, 0, ip, port);
            if (ack_packet == null) {
                session_socket.close();
                return;
            }


            //stream to create file byte[]
            ByteArrayOutputStream stream = new ByteArrayOutputStream();

            int block_num = 1;
            boolean stay = true;
            DatagramPacket data_packet;
            while (stay) {

                data_packet = accept_data_packet(session_socket, ack_packet, ip, port, block_num);

                if (data_packet == null) {
                    close_all_streams(ip_str, port, session_socket, stream);
                    return;
                }


                //calculate the length of the data
                int length = data_packet.getLength() - 4;

                System.out.printf("Client Session: %s %d | Block %d received\n", ip_str, port, block_num);


                // checks to see if this is the last block if so, sets the flag to exit the loop
                if (length < 512) {
                    System.out.printf("Client Session: %s %d | Hit last block \n", ip_str, port);
                    stay = false;
                }


                //writes the new data to the stream
                stream.write(data_packet.getData(), 4, length);

                //sends the ack packet
                ack_packet = send_ack_packet(session_socket, block_num, ip, port);
                if (ack_packet == null) {
                    close_all_streams(ip_str, port, session_socket, stream);
                    return;
                }


                System.out.printf("Client Session: %s %d | Ack %d sent\n", ip_str, port, block_num);

                block_num++;
            }

            close_all_streams(ip_str, port, session_socket, stream);

            byte[] file_bytes = stream.toByteArray();



            file_lock.writeLock().lock();
            try {
                FileOutputStream fos = new FileOutputStream(filepath);
                fos.write(file_bytes);
                fos.close();
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not save file due to IOException | Terminating session | %s\n", ip_str, port, e.getMessage());
                session_socket.close();
                close_all_streams(ip_str, port, session_socket, stream);
                return;
            } catch (Exception e) {
                System.err.printf("Client Session: %s %d | Failed to acquire lock and write to file | %s\n", ip_str, port, e.getMessage());
            } finally {
                file_lock.writeLock().unlock();
            }


            map_lock.lock();
            try {
                Entry<Integer, ReentrantReadWriteLock> entry = file_map.get(filepath);
                int num_of_access = entry.getKey();

                if (num_of_access < 1) file_map.remove(filepath);
                else {
                    entry = new AbstractMap.SimpleImmutableEntry<>(num_of_access - 1, file_lock);
                    file_map.put(filepath, entry);
                }

                System.out.printf("Client Session: %s %d | Acquired lock and wrote to file map\n", ip_str, port);
            } catch (Exception e) {
                System.err.printf("Client Session: %s %d | Failed to acquire lock and read from file map | %s\n", ip_str, port, e.getMessage());
                session_socket.close();
                return;
            } finally {
                map_lock.unlock();
            }

            System.out.printf("Client Session: %s %d | Write request completed | File size : %d bytes\n", ip_str, port, file_bytes.length);
        }
    }

    private static int check_packet_for_data(DatagramSocket session_socket, DatagramPacket packet, DatagramPacket last_packet, InetAddress ip, int port, int block_num) {
        String ip_str = ip.toString().substring(1);

        //first check ip and port
        //second check for code *don't short circuit for code 2
        //third check for block
        //edge case: if code 2 and block 1 -> client didn't receive 0th ack -> retransmit 0th ack

        if (!packet.getAddress().equals(ip) || packet.getPort() != port) {
            System.out.printf("Client Session: %s %d | Ip or port mismatch | Packet dropped\n", ip_str,  port);
            return 2;
        }

        if (decode_code(packet.getData(), false) != 3 && decode_code(packet.getData(), false) != 2 ) {
            System.out.printf("Client Session: %s %d | Code mismatch | Packet dropped\n", ip_str,  port);
            return 2;
        }

        if ((decode_code(packet.getData(), false) == 2 && block_num == 1) || decode_code(packet.getData(), true) == block_num - 1 ) {
            System.out.printf("Client Session: %s %d | Block mismatch | Retransmitting last ack packet\n", ip_str,  port);
            try {
                session_socket.send(last_packet);
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not retransmit ack due to IOException | Terminating session | %s\n", ip_str,  port, e.getMessage());
                return -1;
            }
            return 3;
        }

        if (decode_code(packet.getData(), true) != block_num - 1 && decode_code(packet.getData(), true) != block_num)  {
            System.out.printf("Client Session: %s %d | Block mismatch | Packet dropped\n", ip_str,  port);
            return 2;
        }

        return 1;
    }

    private static DatagramPacket generate_ack_packet(int block_num, InetAddress ip, int port)  {
        String ip_str = ip.toString().substring(1);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            stream.write(decode_short_to_unsigned_bytes(4));
            stream.write(decode_short_to_unsigned_bytes(block_num));
        } catch (IOException e) {
            System.err.printf("Client Session: %s %d | Could not generate ack packet due to IOException | Terminating session | %s\n", ip_str,  port, e.getMessage());
            return null;
        }

        byte[] packet_data = stream.toByteArray();
        return new DatagramPacket(packet_data, packet_data.length, ip,port);
    }

    private static DatagramPacket send_ack_packet(DatagramSocket session_socket, int block_num, InetAddress ip, int port) {
        String ip_str = ip.toString().substring(1);
        DatagramPacket ack = generate_ack_packet(block_num, ip, port);
        if (ack == null) return null;

        try {
            session_socket.send(ack);
        } catch (IOException e) {
            System.err.printf("Client Session: %s %d | Could not send ack packet due to IOException | Terminating session | %s\n", ip_str,  port, e.getMessage());
            return null;
        }

        return ack;
    }

    private static DatagramPacket accept_data_packet(DatagramSocket session_socket, DatagramPacket ack_packet, InetAddress ip, int port, int block_num) {
        String ip_str = ip.toString().substring(1);
        int count = 0;
        while (true){
            if (count > 10) {
                System.err.printf("Client Session: %s %d | Too many attempts to retransmit | Terminating session\n", ip_str,  port);
                return null;
            }

            //packet for receiving data
            byte[] data_packet_buffer = new byte[516];
            DatagramPacket data_packet = new DatagramPacket(data_packet_buffer, data_packet_buffer.length);

            //attempts to receive data
            try {
                session_socket.receive(data_packet);
            } catch (SocketTimeoutException e) {
                System.err.printf("Client Session: %s %d | Client timed out | Terminating session\n", ip_str,  port);
                return null;
            } catch (IOException e) {
                System.err.printf("Client Session: %s %d | Could not receive data packet due to IOException | Terminating session | %s\n", ip_str,  port, e.getMessage());
                return null;
            }

            //checks the packet to see if it received the right packet
            int check_result = check_packet_for_data(session_socket, data_packet, ack_packet, ip, port, block_num);

            switch (check_result) {
                case -1: return null;
                case 1: return data_packet;
                case 3: count++;
            }
        }
    }
}
