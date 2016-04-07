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

public class FileServer {
	public static String host = "";
	public static Integer port = 0;
	public static ZooKeeper zk;
	public static ZkConnector zkc;
	public static Socket jobSocket = null;
	public static ObjectOutputStream out = null;
	public static ObjectInputStream in = null;

	public static List<String> clients;
	public static String myPath = "/f";
	public static String myIp = "localhost";
	public static Integer myPort = 7000;
	public static Watcher watcher;

	public static ArrayList<String> dictionary = new ArrayList<String>();
	public static ArrayList<String> partition = new ArrayList<String>();

	public static CountDownLatch modeSignal = new CountDownLatch(1);

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
		readDictionary();

		while(listening) {
			new FileServerHandlerThread(dictionary, serverSocket.accept()).start();
		}

		serverSocket.close();
	}

	// register file server's ip and port to zookeeper
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

	// store the dictionary file (passed as argument)
	public static void readDictionary() {
		try{
			// find out current working directory
			//String workingDir = System.getProperty("user.dir");
	   		//System.out.println("Current working directory : " + workingDir);

			BufferedReader br = new BufferedReader(new FileReader("/nfs/ug/homes-5/r/riehseun/ece419/lab4/src/dictionary/lowercase.rand"));
            
			String line = "";
			int i = 0;
			while((line = br.readLine()) != null){
			    dictionary.add(i,line);
			    i++;
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}