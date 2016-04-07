import java.io.Serializable;

public class TaskPacket implements Serializable {
	public static final int JOB_REQUEST = 100;
	public static final int JOB_SUBMIT = 101;
	public static final int JOB_QUERY = 102;

	public int type;
	public String job;
}
