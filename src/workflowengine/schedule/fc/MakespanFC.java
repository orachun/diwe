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
public class MakespanFC implements FC
{

    @Override
    public double getFitness(Schedule sch)
    {
        return sch.getMakespan();
    }

    @Override
    public Schedule getBetterSchedule(Schedule s1, Schedule s2)
    {
        if(s1 == null)
        {
            return s2;
        }
        if(s2 == null)
        {
            return s1;
        }
        return s1.getMakespan() < s2.getMakespan() ? s1 : s2;
    }
    
	@Override
	public boolean isScheduleBetter(Schedule s1, Schedule s2)
	{
		return s1 == getBetterSchedule(s1, s2);
	}
}
