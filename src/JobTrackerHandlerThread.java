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
import org.apache.zookeeper.KeeperException.Code;

import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.io.IOException;

import java.net.*;
import java.io.*;
import java.util.*;

public class JobTrackerHandlerThread extends Thread {
	public static Socket socket = null;

	public JobTrackerHandlerThread(Socket socket) {
		super("JobTrackerHandlerThread");
		this.socket = socket;
		System.out.println("Created new Thread to handle JobTracker");
	}

	public void run() {
		ObjectInputStream fromClientDriver;
		ObjectOutputStream toClientDriver;
		try {
			/* stream to read  */
			fromClientDriver = new ObjectInputStream(socket.getInputStream());
			JobPacket packetFromClientDriver;
			
			/* stream to write */
			toClientDriver = new ObjectOutputStream(socket.getOutputStream());
			
			while (( packetFromClientDriver = (JobPacket) fromClientDriver.readObject()) != null) {
				// if client types "job" + "hash"
				if (packetFromClientDriver.type == JobPacket.JOB_REQUEST) {
					JobPacket packetToClientDriver = new JobPacket();
                    packetToClientDriver.type = JobPacket.JOB_SUBMIT;
					toClientDriver.writeObject(packetToClientDriver); 

					JobTracker.create_hash(packetFromClientDriver.hash);
					JobTracker.create_partition_id();
					
					continue; 
				}

				// if client types "status" + "hash"
				if (packetFromClientDriver.type == JobPacket.JOB_CHECK) {
					JobPacket packetToClientDriver = new JobPacket();
					packetToClientDriver.type = JobPacket.JOB_REPLY;

					String result = JobTracker.getData(packetFromClientDriver.hash);
					if (result.equals("No hash found")) {
						packetToClientDriver.result = "No hash found";
					}
					else if (result.equals("Password failed")) {
						packetToClientDriver.result = "Password failed";
						packetToClientDriver.reason = "fucked";
					}
					else if (result == null) {
						packetToClientDriver.result = "In progress";
					}
					else {
						packetToClientDriver.result = result;
					}

					toClientDriver.writeObject(packetToClientDriver); 

					continue; 
				}

				/* if code comes here, there is an error in the packet */
				System.err.println("ERROR: Unknown ECHO_* packet!!");
				System.exit(-1);
			}
			
			/* cleanup when client exits */
			fromClientDriver.close();
			toClientDriver.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
