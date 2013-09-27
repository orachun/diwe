/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

/**
 *
 * @author Orachun
 */
public interface Scheduler
{
//    public Schedule getSchedule(Workflow wf, ExecSite nw);
//    public Schedule getSchedule(Workflow wf, ExecSite nw, HashMap<Task, Worker> fixedMapping);
    public Schedule getSchedule(SchedulerSettings settings);
}
