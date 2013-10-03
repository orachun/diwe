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
	public final String wfDir;

	public ScheduleEntry(String taskUUID, String target, String wfDir)
	{
		this.taskUUID = taskUUID;
		this.target = target;
		this.wfDir = wfDir;
	}
	
}
