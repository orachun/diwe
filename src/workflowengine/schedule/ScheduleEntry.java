/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

/**
 *
 * @author orachun
 */
public class ScheduleEntry
{
	public final String taskUUID;
	public final String target;

	public ScheduleEntry(String taskUUID, String target)
	{
		this.taskUUID = taskUUID;
		this.target = target;
	}
	
}
