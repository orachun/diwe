/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;

/**
 *
 * @author orachun
 */
public class CircularScheduler implements Scheduler
{

    @Override
    public Schedule getSchedule(SchedulingSettings settings)
    {
        Schedule s = new Schedule(settings);
		String[] sites = settings.siteArray();
		String[] tasks = settings.taskArray();
		
        for (int i = 0; i < tasks.length; i++)
		{
			s.setWorkerForTask(tasks[i], sites[i % sites.length]);
		}
        s.evaluate();
        return s;
    }

}
