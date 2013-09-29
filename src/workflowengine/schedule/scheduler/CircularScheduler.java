/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.scheduler.Scheduler;

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
        int totalTasks = settings.getTotalTasks();
        int totalWorkers = settings.getTotalWorkers();
        for(int i=0;i<totalTasks;i++)
        {
            s.setWorkerForTask(i, i%totalWorkers);
        }
        s.evaluate();
        return s;
    }

}
