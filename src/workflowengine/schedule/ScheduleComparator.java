/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.Comparator;

/**
 *
 * @author orachun
 */
public class ScheduleComparator implements Comparator<Schedule>
{
    private Schedule best = null;
    private boolean lessComesFirst = true;
    public ScheduleComparator(){}
    public ScheduleComparator(boolean lessComesFirst)
    {
        this.lessComesFirst = lessComesFirst;
    }
    public int compare(Schedule o1, Schedule o2)
    {
        if (o1.getFitness() == o2.getFitness())
        {
            return 0;
        }
        
        
        if ((lessComesFirst && o1.getFitness() < o2.getFitness())
                || (!lessComesFirst && o1.getFitness() > o2.getFitness())
                )
        {
            saveBest(o1);
            return -1;
        }
        else
        {
            saveBest(o2);
            return 1;
        }
    }
    
    public void saveBest(Schedule s)
    {
        if(best == null 
                    || (lessComesFirst && best.getFitness() > s.getFitness()) 
                    || (!lessComesFirst && best.getFitness() < s.getFitness())
                    )
        {
            best = s.copy();
        }
    }

    public Schedule getBest()
    {
        return best;
    }
    
}
