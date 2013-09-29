package workflowengine.schedule.scheduler;

import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.scheduler.Scheduler;
import static java.lang.Math.exp;
import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.utils.Utils;

public class SA implements Scheduler
{
    public static final String PROP_START_TEMP = "sa_start_temp";
    public static final String PROP_STOP_TEMP = "sa_stop_temp";
    public static final String PROP_INNER_COUNT = "sa_inner_count";
    public static final String PROP_DECREASE_RATE = "sa_decrease_rate";
    
    static int START_TEMP = 100;
    static int STOP_TEMP = 1;
    static int INNER_COUNT = 10;
    static int DECREASE_RATE = 1;
    static final Random r = new Random();
    
    Schedule bestSolution;
    double bestFit = Double.POSITIVE_INFINITY;
    
    public SA()
    {
        Utils.setPropIfNotExist(PROP_START_TEMP, "100");
        Utils.setPropIfNotExist(PROP_STOP_TEMP, "1");
        Utils.setPropIfNotExist(PROP_INNER_COUNT, "5");
        Utils.setPropIfNotExist(PROP_DECREASE_RATE, "1");
        START_TEMP = Utils.getIntProp(PROP_START_TEMP);
        STOP_TEMP = Utils.getIntProp(PROP_STOP_TEMP);
        INNER_COUNT = Utils.getIntProp(PROP_INNER_COUNT);
        DECREASE_RATE = Utils.getIntProp(PROP_DECREASE_RATE);
        
    }
    
    @Override
    public Schedule getSchedule(SchedulingSettings settings)
    {
        bestSolution = null;
        int curTemp;
        double curFit;
        Schedule solution ;
        if(settings.hasParam("init_schedule"))
        {
            solution = (Schedule)settings.getObjectParam("init_schedule");
        }
        else
        {
            solution = new Schedule(settings);
            solution.random();
        }
        curFit = solution.getFitness();
        bestFit = curFit;
        bestSolution = solution.copy();
        for (curTemp = START_TEMP; curTemp > STOP_TEMP; curTemp -= DECREASE_RATE)
        {
            for (int i = 0; i < INNER_COUNT; i++)
            {
                Schedule tempSolution = solution.copy();
                double tempFit;
                slightChange(tempSolution, settings);
                tempFit = tempSolution.getFitness();
                if (tempFit < bestFit)
                {
                    bestFit = tempFit;
                    bestSolution = tempSolution.copy();
                }
                //printSol(tempSolution);
                //System.out.println(tempFit);
                if ((tempFit < curFit) || (r.nextDouble() < bolzMan(tempFit - curFit, curTemp)))
                {
                    curFit = tempFit;
                    solution = tempSolution.copy();
                }
            }
//            System.out.println(bestFit);
        }
        bestSolution.evaluate();
        return bestSolution;
    }

    double bolzMan(double delta, double temp)
    {
        double prob = exp(-delta / temp);
        return prob;
    }

    double randomP(double pmin, double pmax)
    {
        double interval = pmax - pmin;
        double value = r.nextDouble() * interval;
        return pmin + value;
    }


    void slightChange(Schedule sch, SchedulingSettings ss)
    {
        for (int k = 0; k < ss.getTotalTasks(); k++)
        {

            int wkId = ss.getWorkerIndex(sch.getWorkerForTask(k));
            if (r.nextBoolean())
            {
                wkId++;
                wkId = Math.min(wkId, ss.getTotalWorkers() - 1);
            }
            else
            {
                wkId--;
                wkId = Math.max(wkId, 0);
            }
            sch.setWorkerForTask(k, wkId);
        }
    }
}