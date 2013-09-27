/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import workflowengine.workflow.Task;

/**
 *
 * @author orachun
 */
public class ScheduleEntry
{
	public final Task task;
	public final String target;

	public ScheduleEntry(Task task, String target)
	{
		this.task = task;
		this.target = target;
	}
	
}
