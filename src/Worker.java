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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

public class Worker {
	public static String host = "";
	public static Integer port = 0;
	public static ZooKeeper zk;
	public static ZkConnector zkc;
	
	public static void main(String[] args) throws IOException {
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

		ExecutorService executor = Executors.newFixedThreadPool(1);
        executor.execute(new WorkerHandlerThread(zk));
        
	}
}