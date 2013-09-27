/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

/**
 *
 * @author udomo
 */
public class RandomScheduler implements Scheduler
{    
    @Override
    public Schedule getSchedule(SchedulerSettings settings)
    {
        Schedule s = new Schedule(settings);
        s.random();
        return s;
    }
}
