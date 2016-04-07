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

public class JobTracker {
	public static String host = "";
	public static Integer port = 0;
	public static ZooKeeper zk;
	public static ZkConnector zkc;
	public static ObjectOutputStream out = null;
	public static ObjectInputStream in = null;

	public static String myPath = "/j";
	public static String myIp = "localhost";
	public static Integer myPort = 8000;
	public static Watcher watcher;

	public static String jobPath = "/c";
	public static List<String> clients;

	public static void main(String[] args) throws IOException {
		ServerSocket serverSocket = null;
		boolean listening = true;

        if(args.length == 2) {
			host = args[0];
			port = Integer.parseInt(args[1]);
			serverSocket = new ServerSocket(myPort);
		} 
		else {
			System.err.println("ERROR: Invalid arguments!");
			System.exit(-1);
		}
		
		// connect to ZooKeeper
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

		// create watcher
		watcher = new Watcher() { 
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    register();

		while(listening) {
			new JobTrackerHandlerThread(serverSocket.accept()).start();
		}
		
		serverSocket.close();
	}

	// register jobtracker's ip and port to zookeeper
	public static void register() {
		try {
			if (zk.exists(myPath, watcher) == null) {
            	zk.create(
	                myPath,         // Path of znode
	                null,           // Data not needed.
	                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
	                CreateMode.PERSISTENT   // Znode type, set to Persistent.
	                );
            }	
            System.out.println("Creating file server at: " + myPath + ", with port: " + myPort + ", with ip: " + myIp);
            zk.create(
                myPath + "/" + myIp + " " + myPort,         // Path of znode
                null,           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.PERSISTENT   // Znode type, set to Persistent.
                );

        } 
        catch(KeeperException e) {
            System.out.println(e.code());
        } 
        catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
	}

	// crate a node in zookeeper with the hash name under the "c" directory
	public static void create_hash(String hash) { 
		try {
            System.out.println("Creating client at: " + jobPath);

            if (zk.exists(jobPath, watcher) == null) {
            	zk.create(
	                jobPath,         // Path of znode
	                null,           // Data not needed.
	                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
	                CreateMode.PERSISTENT   // Znode type, set to Persistent.
	                );
            }	 
            zk.create(
                jobPath + "/" + hash,         // Path of znode
                null,           // Data not needed.
                Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
                CreateMode.PERSISTENT   // Znode type, set to Persistent.
                );
			
			System.out.println("Created node: " + jobPath + "/" + hash);
        } 
        catch(KeeperException e) {
            System.out.println(e.code());
        } 
        catch(Exception e) {
            System.out.println("Make node:" + e.getMessage());
        }
	}

	// for each directory in "c" directory, create three more directories whose names represent the partition ids
	public static void create_partition_id() {
		try {		
			clients = zk.getChildren(jobPath, watcher);	 
			if (clients.isEmpty()) {
				System.out.println("No clients");
			}
			else {
				System.out.println("hashes: " + clients);
				for (int j=0; j<clients.size(); j++) {
					System.out.println("Partitioning " + jobPath + "/" + clients.get(j));
					for (int i=1; i<=3; i++) {
						try {
					        zk.create(
					            jobPath + "/" + clients.get(j) + "/" + i,         // Path of znode
					            String.valueOf(0).getBytes(),           // Data: "Not assigned"
					            Ids.OPEN_ACL_UNSAFE,    // ACL, set to Completely Open.
					            CreateMode.PERSISTENT   // Znode type, set to Persistent.
					            );

					    } 
					    catch(KeeperException e) {
					        System.out.println(e.code());
					    } 
					    catch(Exception e) {
					        System.out.println("Make node:" + e.getMessage());
					    }
					}
				}
			}
		} 
		catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// get data bit from specific partition in specific hash
    public static String getData(String hash) {
    	watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };
 
 		byte[] data = null;
		try {		
			clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash;
			
			if (zk.exists(newPath, watcher) == null) {
	        	return "No hash found";
	        }	

	        System.out.println("newPath: " + newPath);
			data = zk.getData(newPath, false, null);
			
			if (clients.isEmpty()) {
				System.out.println("No clients");
			}
		} 
		catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("data: " + byteToString(data));
		return byteToString(data);
    }

    // convert byte to string
    public static String byteToString(byte[] b) {
		String s = null;
		if (b != null) {
			try {
				s = new String(b, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}
		return s;
	}
}