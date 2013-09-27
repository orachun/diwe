/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.Serializable;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class TaskStatus implements Serializable
{
    public static final char STATUS_WAITING = 'W';
    public static final char STATUS_EXECUTING = 'E';
    public static final char STATUS_COMPLETED = 'C';
    public static final char STATUS_FAIL = 'F';
	
	public String taskUUID;
	public char status;
	public int retVal;
	public String errMsg;
	public long start = -1;
	public long finish = -1;

	public TaskStatus(Task t, char status, int retVal, String errMsg)
	{
		this(t.getUUID(), status, retVal, errMsg);
	}
	public TaskStatus(String taskUUID, char status, int retVal, String errMsg)
	{
		this.taskUUID = taskUUID;
		this.status = status;
		this.retVal = retVal;
		this.errMsg = errMsg;
	}

	public TaskStatus(String taskUUID, char status, int retVal, String errMsg, long start, long finish)
	{
		this.taskUUID = taskUUID;
		this.status = status;
		this.retVal = retVal;
		this.errMsg = errMsg;
		this.start = start;
		this.finish = finish;
	}
	
	
	
	public static TaskStatus waitingStatus(Task t)
	{
		return new TaskStatus(t.getUUID(), STATUS_WAITING, -1, "");
	}
	public static TaskStatus executingStatus(Task t)
	{
		return new TaskStatus(t.getUUID(), STATUS_EXECUTING, -1, "", Utils.time(), -1);
	}
	public static TaskStatus completedStatus(TaskStatus ts)
	{
		return new TaskStatus(ts.taskUUID, STATUS_COMPLETED, 0, "", ts.start, Utils.time());
	}
	public static TaskStatus failedStatus(TaskStatus ts, int retVal, String errMsg)
	{
		return new TaskStatus(ts.taskUUID, STATUS_FAIL, retVal, errMsg, ts.start, Utils.time());
	}
	
	public TaskStatus complete()
	{
		return new TaskStatus(this.taskUUID, STATUS_COMPLETED, 0, "", this.start, Utils.time());
	}
	public TaskStatus fail(int retVal, String errMsg)
	{
		return new TaskStatus(this.taskUUID, STATUS_FAIL, retVal, errMsg, this.start, Utils.time());
	}
}
