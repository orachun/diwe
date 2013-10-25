/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.fc;

import workflowengine.schedule.Schedule;

/**
 *
 * @author orachun
 */
public interface FC
{
    public double getFitness(Schedule sch);
    public Schedule getBetterSchedule(Schedule s1, Schedule s2);
	public boolean isScheduleBetter(Schedule s1, Schedule s2);
}
