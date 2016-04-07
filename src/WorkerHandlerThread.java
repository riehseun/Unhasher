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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.math.BigInteger;

import java.net.*;
import java.io.*;
import java.util.*;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WorkerHandlerThread extends Thread {
	public static String host = "";
	public static Integer port = 0;
	public static ZooKeeper zk;
	public static ZkConnector zkc;
	public static Socket socket = null;
	public static ObjectOutputStream out = null;
	public static ObjectInputStream in = null;
	
	public static List<String> fileServers;
	public static String jobPath = "/c";
	public static String filePath = "/f";
	public static Watcher watcher;
	public static List<String> clients;

	public static String result;

	public static String myPath = "/f";
	public static String myIp = "localhost";
	public static Integer myPort = 7000;
	public static CountDownLatch modeSignal = new CountDownLatch(1);

	public static Lock _mutex = new ReentrantLock(true);

	public WorkerHandlerThread(ZooKeeper zk) {
		super("created new WorkerHandlerThread");
		this.zk = zk;
		System.out.println("created new WorkerHandlerThread");
	}

	public void run() {
	    // check if file server is not empty and try to initialize status bits and create connection to file server
		// create watcher
		watcher = new Watcher() { 
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	            handleEvent(event);
	    	}
	    };

		try {		
			fileServers = zk.getChildren(filePath, watcher);	 
			if (fileServers.isEmpty()) {
				System.out.println("No FileServer");
			}
			else {
				_mutex.lock();
				init();
				_mutex.unlock();
				// if detect file server fail, connect to back up
				connectToFileServer(fileServers.get(0), 1);	
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
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	// create connection to the file server and call appropriate functions.
	public static void connectToFileServer(String fs, int partition_id) throws IOException, ClassNotFoundException, InterruptedException, KeeperException {
		_mutex.lock();
		String hash = findHash();
		// this partitions is on progress
		System.out.println("In progress: " + hash + " in partition: " + partition_id);
		setToInProgress(hash, partition_id);

		String[] command = fs.split("\\s+");
		command[0] = String.format("%s", command[0]);
		Integer.parseInt(command[1]);
		try {
			JobPacket packetToFilerServer = new JobPacket();
			packetToFilerServer.type = JobPacket.JOB_FETCH;
			packetToFilerServer.partition_id = partition_id;
			socket = new Socket(command[0], Integer.parseInt(command[1]));
			out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());
            out.writeObject(packetToFilerServer);

            JobPacket packetFromFileServer;
			packetFromFileServer = (JobPacket) in.readObject();
			if (packetFromFileServer.type == JobPacket.JOB_FETCH_REPLY) {
				//System.out.println("received partition from file server: ");
				//System.out.println(packetFromFileServer.partition);
				//System.out.println("working on hash: " + hash + ", partition: " + partition_id);
				if (checkPartition(packetFromFileServer.partition, hash) == true) {
					//Worker.start();
					//System.out.println("password found! for hash: " + hash + " in partition: " + partition_id + " pwd: " + result);
					// set all partitions for this hash to "found!"
					setToFound(hash, partition_id);
					passwordFound(result, hash);
					partition_id++;
					// work on the next partition
					if (partition_id <= 3) {
						connectToFileServer(fs, partition_id);
					}
					else {
						partition_id = 1;
						if (findHash() != "") {
							connectToFileServer(fs, partition_id);
						}
						else {
							setFailed();
							System.exit(0);
						} 
					}
				}
				else {
					//System.out.println("password not found! for hash: " + hash + " in partition: " + partition_id);
					// not found for this partition
					setToNotFound(hash, partition_id);
					// work on the next partition
					partition_id++;
					if (partition_id <= 3) {
						connectToFileServer(fs, partition_id);
					}
					else {
						// if there are still partitions to be worked on 
						partition_id = 1;
						if (findHash() != "") {
							connectToFileServer(fs, partition_id);
						}
						// if every partition is finished
						else {
							setFailed();
							System.exit(0);
						} 
					}
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
		_mutex.unlock();
	}

	public static void setFailed() {
		boolean found = false;

		watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };
 
		try {		
			clients = zk.getChildren(jobPath, watcher);	 
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

		for (int i=0; i<clients.size(); i++) {
			found = false;
			for (int j=1; j<=3; j++) {
				if (getData(clients.get(i), j) == 2) {
					found = true;
				}
			}
			if (found == false) {
				passwordFailed(clients.get(i));
				System.out.println("password does not exist! for hash: " + clients.get(i) + " in All partitions");
			}
			else {
				System.out.println("password exists! for hash: " + clients.get(i) + " in one partition");
			}
		}
	}

	// initialize status bits for all partitions for all nodes
	public static void init() {
		watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };
 
		try {		
			clients = zk.getChildren(jobPath, watcher);	 
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

		for (int i=0; i<clients.size(); i++) {
			for (int j=1; j<=3; j++) {
				setToNotAssigned(clients.get(i), j);
			}
		}
	}

	// get hash using word
	public static String getHash(String word) {
        String hash = null;
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            BigInteger hashint = new BigInteger(1, md5.digest(word.getBytes()));
            hash = hashint.toString(16);
            while (hash.length() < 32) hash = "0" + hash;
        } 
        catch (NoSuchAlgorithmException nsae) {
            // ignore
        }
        return hash;
    }

    // check hash matches in a partition
    public static boolean checkPartition(ArrayList<String> partition, String hashFromCliennt) {
    	for (int i=0; i<partition.size(); i++) {
    		String word = partition.get(i);
    		String hash = getHash(word);
    		if(hash.equals(hashFromCliennt)) {	
    			result = word;
				return true;
		    }
    	}
    	return false;
    }

    // find unfinished partitions on ALL hashes
    public static String findHash() {
    	watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };
 
		try {		
			clients = zk.getChildren(jobPath, watcher);	 
			if (clients.isEmpty()) {
				System.out.println("No clients");
			}
		} 
		catch (KeeperException e) {
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		for (int i=0; i<clients.size(); i++) {
			for (int j=1; j<=3; j++) {
				// not assigned or in progress
				if (getData(clients.get(i), j) == 0) {
					return clients.get(i);
				}
			}
		}
		return "";
    }

    // get data bit from specific partition in specific hash
    public static int getData(String hash, int partition_id) {
    	String.valueOf(partition_id);

    	watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };
 
 		byte[] data = null;
		try {		
			clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash + "/" + partition_id;
			data = zk.getData(newPath, false, null);
			
			if (clients.isEmpty()) {
				System.out.println("No clients");
			}
		} 
		catch (KeeperException e) {
			e.printStackTrace();
		} 
		catch (InterruptedException e) {
			e.printStackTrace();
		}

		int retVal = Integer.parseInt(byteToString(data));
		return retVal;
    }

    // set data bit to 0 from specific partition in specific hash
    public static void setToNotAssigned(String hash, int partition_id) {
    	String.valueOf(partition_id);

    	watcher = new Watcher() { 
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    try {
	    	clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash + "/" + partition_id;
	        zk.setData(
	            newPath,         // Path of znode
	            String.valueOf(0).getBytes(),          // Data: "Not assigned"
	            -1
	            );
	        //System.out.println("setToNotAssigned: "  + partition_id + ", bit: " + getData(hash, partition_id));
	    } 
	    catch(KeeperException e) {
	        System.out.println(e.code());
	    } 
	    catch(Exception e) {
	        System.out.println("(setToNotAssigned) Make node:" + e.getMessage());
	    }
    }

    // set data bit to 1 from specific partition in specific hash
    public static void setToInProgress(String hash, int partition_id) {
    	String.valueOf(partition_id);

    	watcher = new Watcher() { 
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    try {
	    	clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash + "/" + partition_id;
	        zk.setData(
	            newPath,         // Path of znode
	            String.valueOf(1).getBytes(),           // Data: "In progress"
	            -1
	            );
	        //System.out.println("setToInProgress: "  + partition_id + ", bit: " + getData(hash, partition_id));
	    } 
	    catch(KeeperException e) {
	        System.out.println(e.code());
	    } 
	    catch(Exception e) {
	        System.out.println("(setToInProgress) Make node:" + e.getMessage());
	    }
    }

    // set data bit to 2 from specific partition in specific hash
    public static void setToFound(String hash, int partition_id) {
    	String.valueOf(partition_id);

    	watcher = new Watcher() { 
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    try {
	    	clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash + "/" + partition_id;
	        zk.setData(
	            newPath,         // Path of znode
	            String.valueOf(2).getBytes(),           // Data: "Password found"
	            -1
	            );
	        //System.out.println("setToFound: "  + partition_id + ", bit: " + getData(hash, partition_id));
	    } 
	    catch(KeeperException e) {
	        System.out.println(e.code());
	    } 
	    catch(Exception e) {
	        System.out.println("(setToFound) Make node:" + e.getMessage());
	    }
    }

    // set data bit to 3 from specific partition in specific hash
    public static void setToNotFound(String hash, int partition_id) {
    	String.valueOf(partition_id);

    	watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    try {
	    	clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash + "/" + partition_id;
	        zk.setData(
	            newPath,         // Path of znode
	            String.valueOf(3).getBytes(),           // Data: "Password not found"
	            -1
	            );
	        //System.out.println("setToNotFound: "  + partition_id + ", bit: " + getData(hash, partition_id));
	    } 
	    catch(KeeperException e) {
	        System.out.println(e.code());
	    } 
	    catch(Exception e) {
	        System.out.println("(setToNotFound) Make node:" + e.getMessage());
	    }
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

	// put password in zookeeper for specific hash
    public static void passwordFound(String password, String hash) {
    	watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    try {
	    	clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash;
	        zk.setData(
	            newPath,         // Path of znode
	            String.valueOf(password).getBytes(),    // Data: password
	            -1
	            );
	        //System.out.println("password: " + password + " sent!");
	    } 
	    catch(KeeperException e) {
	        System.out.println(e.code());
	    } 
	    catch(Exception e) {
	        System.out.println("(passwordFound) Make node:" + e.getMessage());
	    }
    }

    // put "passwordFailed" in zookeeper for specific hash
    public static void passwordFailed(String hash) {
    	watcher = new Watcher() { // Anonymous Watcher
	        @Override
	        public void process(WatchedEvent event) {
	            System.out.println("creating watcher!");
	    	}
	    };

	    try {
	    	clients = zk.getChildren(jobPath, watcher);	 
			String newPath = jobPath + "/" + hash;
	        zk.setData(
	            newPath,         // Path of znode
	            String.valueOf("Password failed").getBytes(),  // Data: "password failed"
	            -1
	            );
	        //System.out.println("Password failed sent!");
	    } 
	    catch(KeeperException e) {
	        System.out.println(e.code());
	    } 
	    catch(Exception e) {
	        System.out.println("(passwordFound) Make node:" + e.getMessage());
	    }
    }


    public static void becomePrimary() {
    	String path = myPath + "/" + myIp + " " + myPort + "/primary";
        Stat stat = zkc.exists(path, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            System.out.println("Creating " + path);
            Code ret = zkc.create(
                        path,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) System.out.println("the primary!");

            modeSignal.countDown();
        } 
    }

    public static void handleEvent(WatchedEvent event) {
    	System.out.println("handleEvent");
    	String pPath = myPath + "/" + myIp + " " + myPort + "/primary";
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(pPath)) {
            if (type == EventType.NodeDeleted) { 
                System.out.println(pPath + " deleted! Let's go!");       
                becomePrimary(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                System.out.println(pPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                becomePrimary(); // re-enable the watch
            }
        }
    }
}
