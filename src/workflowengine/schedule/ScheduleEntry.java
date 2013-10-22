/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.io.Serializable;

/**
 *
 * @author orachun
 */
public class ScheduleEntry implements Serializable
{
	public final String taskUUID;
	public final String target;
	/**
	 * also used as a directory
	 */
	public final String superWfid;

	public ScheduleEntry(String taskUUID, String target, String superWfid)
	{
		this.taskUUID = taskUUID;
		this.target = target;
		this.superWfid = superWfid;
	}
	
}
