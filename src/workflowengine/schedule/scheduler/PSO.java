/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.scheduler.Scheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import removed.ExecSite;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleComparator;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;

/**
 *
 * @author Dew
 */
public class PSO implements Scheduler
{
    public static final String VAR_GBEST = "VAR_GBEST";
    public static final String PROP_ITERATIONS = "pso_iterations";
    public static final String PROP_POP_SIZE = "pso_population_size";
    public static final String PROP_PBEST_WEIGHT = "pso_pbest_weight";
    public static final String PROP_GBEST_WEIGHT = "pso_gbest_weight";
    public static final String PROP_INERTIA_WEIGHT = "pso_inertia_weight";
    
    private static double ITERATIONS;
    private static int POP_SIZE;
    private List<PSOIndividual> population = new ArrayList<>();
    private HashMap<String, Object> globalVars;
    private SchedulingSettings ss;
    private ScheduleComparator scp = new ScheduleComparator();

    public void init(SchedulingSettings ss)
    {
        this.ss = ss;
        Utils.setPropIfNotExist(PROP_ITERATIONS, "100");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "20");
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.3");
        
        ITERATIONS = Utils.getIntProp(PROP_ITERATIONS);
        POP_SIZE = Utils.getIntProp(PROP_POP_SIZE);
        globalVars = new HashMap<>();
        globalVars.put(PSO.VAR_GBEST, null);
        population = new ArrayList<>(POP_SIZE);
        
        
        for (int i = 0; i < POP_SIZE; i++)
        {
            population.add(new PSOIndividual(ss, globalVars));
            population.get(i).random();
        }
        
    }
    
    public void sort()
    {
        Collections.sort(population, scp);
    }
    
    public void step()
    {
        for (int j = 0; j < POP_SIZE; j++)
        {
            population.get(j).calVelocity();
            population.get(j).updatePosition();
            population.get(j).updateFitness();
        }
    }
    
    public List<PSOIndividual> getPop()
    {
        return population;
    }
    
    public Schedule getSchedule(SchedulingSettings ss)
    {
        this.ss = ss;
        init(ss);
        for (int k = 0; k < ITERATIONS; k++)
        {
            step();
        }
        return getBestSchedule();
    }
    
    public Schedule getBestSchedule()
    {
        return (Schedule)globalVars.get(VAR_GBEST);
    }

    /**
     * upperbound is excluded
     *
     * @param taskUpperBound
     * @param taskLowerBound
     * @param resourceBoundLength
     * @param resourceLowerBound
     * @param ss
     * @return
     */
    public Schedule limitBound(int taskUpperBound, int taskLowerBound, int resourceBoundLength, SchedulingSettings ss)
    {
        init(ss);
        ss.setParam("taskUpperBound", taskUpperBound);
        ss.setParam("taskLowerBound", taskLowerBound);
        ss.setParam("resourceBoundLength", resourceBoundLength);
        for (int i = 0; i < POP_SIZE; i++)
        {
            population.add(new PSOIndividual(ss, globalVars));
            population.get(i).random();
        }
        for (int k = 0; k < ITERATIONS; k++)
        {
            for (int j = 0; j < POP_SIZE; j++)
            {
                population.get(j).calVelocity();
                population.get(j).updatePosition();
                population.get(j).updateFitness();
            }
        }
        return (Schedule)globalVars.get(VAR_GBEST);
    }
    public static void main(String[] args)
    {
        Utils.disableDB();
        Workflow wf = Workflow.fromDummyDAX("/home/orachun/WorkflowEngine/dummy-dags/Inspiral_100.xml.dummy", true);
        System.out.println(wf.getCumulatedExecTime());
        ExecSite es = ExecSite.generate(30);
        
        Utils.setPropIfNotExist(PROP_ITERATIONS, "200");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "25");
        //1.9132904503125059
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.6");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.1");
        //1.8252815646875014
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.6");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.1");
        
        //1.9372794681250116
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.45");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.45");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.1");
        
        //1.9212962296875076
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.3");
        
        Scheduler PSO = new PSO();
        SchedulingSettings ss = new SchedulingSettings(wf, es, new CostOptimizationFC(wf.getCumulatedExecTime()*0.1,10,1));
        
        double total = 0;
        for(int i=0;i<10;i++)
        {
            Schedule psos = PSO.getSchedule(ss);
            System.out.println(psos.getFitness());
            System.out.println("\t"+psos.getMakespan());
            System.out.println("\t"+psos.getCost());
            System.out.println("\t"+psos.toString());
            total += psos.getFitness();
        }
        System.out.println(total/10);
    }
}
