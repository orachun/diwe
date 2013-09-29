/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import java.util.LinkedList;
import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;

/**
 *
 * @author udomo
 */
public class RandomScheduler implements Scheduler
{    
    @Override
    public Schedule getSchedule(SchedulingSettings settings)
    {
        Schedule s = new Schedule(settings);
		LinkedList<String> sites = new LinkedList<>(settings.getExecNetwork().getExecutorURISet());
		Random r = new Random();
		for(String taskUUID: settings.getTaskUUIDSet())
		{
			s.setWorkerForTask(taskUUID, sites.get(r.nextInt(sites.size())));
		}
        return s;
    }
}
