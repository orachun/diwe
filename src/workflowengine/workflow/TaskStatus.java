/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.Serializable;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class TaskStatus implements Serializable
{
    public static final char STATUS_DISPATCHED = 'D';
    public static final char STATUS_WAITING = 'W';
    public static final char STATUS_EXECUTING = 'E';
    public static final char STATUS_COMPLETED = 'C';
    public static final char STATUS_FAIL = 'F';
    public static final char STATUS_SUSPENDED = 'S';
	
	public final String taskID;
	public final char status;
	public final int retVal;
	public final String errMsg;
	public final long start;
	public final long finish;

	public final ScheduleEntry schEntry;
	
	
	public TaskStatus(String tid, char status, int retVal, String errMsg, long start, long finish)
	{
		this.taskID = tid;
		this.status = status;
		this.retVal = retVal;
		this.errMsg = errMsg;
		this.start = start;
		this.finish = finish;
		schEntry = null;
	}
	public TaskStatus(ScheduleEntry schEntry, char status, int retVal, String errMsg)
	{
		this.schEntry = schEntry;
		this.taskID = schEntry.taskUUID;
		this.status = status;
		this.retVal = retVal;
		this.errMsg = errMsg;
		this.start = -1;
		this.finish = -1;
	}

	public TaskStatus(ScheduleEntry schEntry, char status, int retVal, String errMsg, long start, long finish)
	{
		this.schEntry = schEntry;
		this.taskID = schEntry.taskUUID;
		this.status = status;
		this.retVal = retVal;
		this.errMsg = errMsg;
		this.start = start;
		this.finish = finish;
	}
	
	public static TaskStatus waitingStatus(String tid)
	{
		return new TaskStatus(tid, STATUS_WAITING, -1, "", -1, -1);
	}
	public static TaskStatus dispatchedStatus(String tid)
	{
		return new TaskStatus(tid, STATUS_DISPATCHED, -1, "", -1, -1);
	}
	public static TaskStatus executingStatus(ScheduleEntry schEntry)
	{
		return new TaskStatus(schEntry, STATUS_EXECUTING, -1, "", Utils.time(), -1);
	}
	public static TaskStatus completedStatus(TaskStatus ts)
	{
		return new TaskStatus(ts.schEntry, STATUS_COMPLETED, 0, "", ts.start, Utils.time());
	}
	public static TaskStatus failedStatus(TaskStatus ts, int retVal, String errMsg)
	{
		return new TaskStatus(ts.schEntry, STATUS_FAIL, retVal, errMsg, ts.start, Utils.time());
	}
	
	public TaskStatus suspend()
	{
		return new TaskStatus(this.schEntry, STATUS_SUSPENDED, -1, "", this.start, -1);
	}
	public TaskStatus complete()
	{
		return new TaskStatus(this.schEntry, STATUS_COMPLETED, 0, "", this.start, Utils.time());
	}
	public TaskStatus complete(int retVal, String errMsg)
	{
		return new TaskStatus(this.schEntry, STATUS_COMPLETED, retVal, errMsg, this.start, Utils.time());
	}
	public TaskStatus fail(int retVal, String errMsg)
	{
		return new TaskStatus(this.schEntry, STATUS_FAIL, retVal, errMsg, this.start, Utils.time());
	}
}
