/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

/**
 *
 * @author orachun
 */
public class CircularScheduler implements Scheduler
{

    @Override
    public Schedule getSchedule(SchedulerSettings settings)
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
