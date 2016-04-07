import java.io.Serializable;
import java.net.*;
import java.io.*;
import java.util.*;

public class JobPacket implements Serializable {
	public static final int JOB_REQUEST = 100;
	public static final int JOB_CHECK = 101;
	public static final int JOB_SUBMIT = 102;
	public static final int JOB_REPLY = 103;

	public static final int JOB_FETCH = 104;
	public static final int JOB_FETCH_REPLY = 105;

	public static final int JOB_QUERY = 102;

	public int type;
	public String hash;
	public int partition_id;

	public ArrayList<String> partition;

	public String result;
	public String reason;
}
