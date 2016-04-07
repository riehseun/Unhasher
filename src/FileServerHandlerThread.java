import java.net.*;
import java.io.*;
import java.util.*;

public class FileServerHandlerThread extends Thread {
	private Socket socket = null;
	public ArrayList<String> dictionary = new ArrayList<String>();
	public ArrayList<String> partition = new ArrayList<String>();

	public FileServerHandlerThread(ArrayList<String> dictionary, Socket socket) {
		super("FileServerHandlerThread");
		this.socket = socket;
		this.dictionary = dictionary;
		System.out.println("Created new Thread to handle FileServer");
	}

	public void run() {
		ObjectInputStream fromWorker = null;
		ObjectOutputStream toWorker = null;
		try {
			/* stream to read */
			fromWorker = new ObjectInputStream(socket.getInputStream());
			JobPacket packetFromWorker;
			
			/* stream to write back to Worker */
			toWorker = new ObjectOutputStream(socket.getOutputStream()); 
			
			while (( packetFromWorker = (JobPacket) fromWorker.readObject()) != null) {
				JobPacket packetToWorker = new JobPacket();
				// sends dictionary partitions to workers on worker requests 
				if (packetFromWorker.type == JobPacket.JOB_FETCH) {
                    packetToWorker.type = JobPacket.JOB_FETCH_REPLY;
                    partition = new ArrayList<String>();
                    int index = (packetFromWorker.partition_id-1)*100000;
                    // 100000 words per partition -> 3 partitiions in total
                    for (int j=index; j<index + 100000 && j<265744; j++) {
                    	partition.add(dictionary.get(j));
                    }
                    System.out.println("partition_size:" + partition.size());
                    System.out.println("partition to give to worker (first word): " + partition.get(0));
                    
                    packetToWorker.partition = partition;
					toWorker.writeObject(packetToWorker); 
					continue; 
				}
				
				/* if code comes here, there is an error in the packet */
				System.err.println("ERROR: Unknown ECHO_* packet!!");
				System.exit(-1);
			}
			
			/* cleanup when client exits */
			fromWorker.close();
			toWorker.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
		
	}
}
