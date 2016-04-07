import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.io.IOException;

import java.io.*;
import java.net.*;

public class ClientDriver {
	public static String host = "";
	public static Integer port = 0;
	public static ZooKeeper zk;
	public static ZkConnector zkc;
	public static ObjectOutputStream out = null;
	public static ObjectInputStream in = null;
	public static Socket socket = null;

	static CountDownLatch nodeCreatedSignal = new CountDownLatch(1);
    static String myPath = "/c";
    public static String filePath = "/j";
   
    public static Watcher watcher;
	public static String jobPath = "/c";
	public static List<String> clients;

    public static List<String> jobTrackers;

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		String host = "";
		Integer port = 0;
		ZooKeeper zk;
		ZkConnector zkc;
		Socket jobSocket = null;
		Socket clientSocket = null;
		ObjectOutputStream out = null;
		ObjectInputStream in = null;
		Watcher watcher;

		if(args.length == 2) {
			host = args[0];
			port = Integer.parseInt(args[1]);
		} 
		else {
			System.err.println("ERROR: Invalid arguments!");
			System.exit(-1);
		}

		//connect to ZooKeeper
		zkc = new ZkConnector();
		try {			
			String hosts = String.format("%s:%d", host, port);
			zkc.connect(hosts);
		} 
		catch (IOException e) {
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		zk = zkc.getZooKeeper();

		watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

		// user interface for submitting job request and job status queries
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String userInput;

		System.out.print("CONSOLE>");
		while ((userInput = stdIn.readLine()) != null && userInput.toLowerCase().indexOf("bye") == -1) {
			// If user presses "x", exit
            if (userInput.equals("x")) {
                System.exit(0);
            }

            String[] command = userInput.split("\\s+");
            if (command[0].equals("job") && command.length == 2) {
            	try {		
					jobTrackers = zk.getChildren(filePath, watcher);	 
					if (jobTrackers.isEmpty()) {
						System.out.println("No JobTrackers");
					}
					else {
						// if detect failures, connect to backup
						requestToJobTracker(jobTrackers.get(0), command[1]);	
					}
				} 
				catch (KeeperException e) {
					e.printStackTrace();
				} 
				catch (InterruptedException e) {
					e.printStackTrace();
				}
				catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
            }

            if (command[0].equals("status") && command.length == 2) {
            	try {		
					jobTrackers = zk.getChildren(filePath, watcher);	 
					if (jobTrackers.isEmpty()) {
						System.out.println("No JobTrackers");
					}
					else {
						// if detect failures, connect to backup
						checkJobTracker(jobTrackers.get(0), command[1]);	
					}
				} 
				catch (KeeperException e) {
					e.printStackTrace();
				} 
				catch (InterruptedException e) {
					e.printStackTrace();
				}
				catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
            }
			
            /* re-print console prompt */
			System.out.print("CONSOLE>");
		}
	}

	public static void requestToJobTracker(String jt, String hash) throws IOException, ClassNotFoundException {
		String[] command = jt.split("\\s+");
		command[0] = String.format("%s", command[0]);
		Integer.parseInt(command[1]);
		try {
			JobPacket packetToJobTracker = new JobPacket();
			packetToJobTracker.type = JobPacket.JOB_REQUEST;
			packetToJobTracker.hash = hash;
			socket = new Socket(command[0], Integer.parseInt(command[1]));
			out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            out.writeObject(packetToJobTracker);

            JobPacket packetFromJobTracker;
			packetFromJobTracker = (JobPacket) in.readObject();
			if (packetFromJobTracker.type == JobPacket.JOB_SUBMIT) {
				System.out.println("Job summitted");
			}
			out.close();
			in.close();
			socket.close();
		}
		catch (IOException e) {
			System.out.println("No files servers running!");
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	}

	public static void checkJobTracker(String jt, String hash) throws IOException, ClassNotFoundException {
		String[] command = jt.split("\\s+");
		command[0] = String.format("%s", command[0]);
		Integer.parseInt(command[1]);
		try {
			JobPacket packetToJobTracker = new JobPacket();
			packetToJobTracker.type = JobPacket.JOB_CHECK;
			packetToJobTracker.hash = hash;
			socket = new Socket(command[0], Integer.parseInt(command[1]));
			out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            out.writeObject(packetToJobTracker);

            JobPacket packetFromJobTracker;
			packetFromJobTracker = (JobPacket) in.readObject();
			if (packetFromJobTracker.type == JobPacket.JOB_REPLY) {
				if (packetFromJobTracker.result.equals("In progress")) {
					System.out.println("In progress");
				}
				else if (packetFromJobTracker.result.equals("Password failed")) {
					System.out.println("Password not found: " + packetFromJobTracker.reason);
				}
				else if (packetFromJobTracker.result.equals("No hash found")) {
					System.out.println("No hash found");
				}
				else {
					System.out.println("Password found!: " + packetFromJobTracker.result);
				}
			}
			out.close();
			in.close();
			socket.close();
		}
		catch (IOException e) {
			System.out.println("No files servers running!");
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	}
}