/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.fc;

import workflowengine.schedule.Schedule;
import workflowengine.utils.Utils;


/**
 *
 * @author orachun
 */
public class CostOptimizationFC implements FC
{
    public static final String PROP_DEADLINE = "fc_deadline";
    public static final String PROP_CONSTANT_PENALTY = "fc_constant_penalty";
    public static final String PROP_WEIGHTED_PENALTY = "fc_weighted_penalty";
    
    private double CONSTANT_PENALTY = 500;
    private double WEIGHT = 100;
    private double DEADLINE = Double.POSITIVE_INFINITY;

    public CostOptimizationFC()
    {
		if(Utils.hasProp(PROP_DEADLINE))
		{
			DEADLINE = Utils.getDoubleProp(PROP_DEADLINE);
		}
		if(Utils.hasProp(PROP_CONSTANT_PENALTY))
		{
			CONSTANT_PENALTY = Utils.getDoubleProp(PROP_CONSTANT_PENALTY);
		}
		if(Utils.hasProp(PROP_WEIGHTED_PENALTY))
		{
			WEIGHT = Utils.getDoubleProp(PROP_WEIGHTED_PENALTY);
		}
    }

    public CostOptimizationFC(double percentDeadline, double constant, double weight)
    {
        this.CONSTANT_PENALTY = constant;
        this.DEADLINE = percentDeadline;
        this.WEIGHT = weight;
    }
    
    @Override
    public double getFitness(Schedule sch)
    {
        double fitness;
        if (sch.getMakespan() > DEADLINE)
        {
            double PenaltyFunction = CONSTANT_PENALTY + WEIGHT * (sch.getMakespan() - DEADLINE*sch.getSettings().getWorkflow().getCumulatedExecTime());
            fitness = sch.getCost() + PenaltyFunction;
        }
        else
        {
            fitness = sch.getCost();
        }
        return fitness;
    }
    
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
        if(s1.getFitness() < s2.getFitness())
        {
            return s1;
        }
        else if(s1.getFitness() > s2.getFitness())
        {
            return s2;
        }
        else 
        {
            return s1.getMakespan() < s2.getMakespan() ? s1 : s2;
        }
    }

	@Override
	public boolean isScheduleBetter(Schedule s1, Schedule s2)
	{
		return s1 == getBetterSchedule(s1, s2);
	}
	
	
}
