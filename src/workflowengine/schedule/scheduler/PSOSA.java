/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.utils.ModifiedFixedThreadPool;
import workflowengine.utils.UniqueMap;
import workflowengine.utils.Utils;

/**
 *
 * @author Dew
 */
public class PSOSA implements Scheduler
{
    public static final String VAR_GBEST = "VAR_GBEST";
    public static final String PROP_ITERATIONS = "pso_iterations";
    public static final String PROP_POP_SIZE = "pso_population_size";
    public static final String PROP_PBEST_WEIGHT = "pso_pbest_weight";
    public static final String PROP_GBEST_WEIGHT = "pso_gbest_weight";
    public static final String PROP_INERTIA_WEIGHT = "pso_inertia_weight";
    
    private static double ITERATIONS;
    private static int POP_SIZE;
    private PSOIndividual[] population;
    private final HashMap<String, Object> globalVars = new HashMap<>();

    public void init(SchedulingSettings ss)
    {
		String[] workers = ss.getSiteArray();
		ss.setParam("workers", workers);
		ss.setParam("tasks", ss.getTaskArray());
		ss.setParam("workerMap", UniqueMap.fromArray(workers));
		
		
        Utils.setPropIfNotExist(PROP_ITERATIONS, "100");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "20");
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.3");
        
        ITERATIONS = Utils.getIntProp(PROP_ITERATIONS);
        POP_SIZE = Utils.getIntProp(PROP_POP_SIZE);
        globalVars.put(PSOSA.VAR_GBEST, null);
        population = new PSOIndividual[POP_SIZE];
    }
    
	@Override
    public Schedule getSchedule(final SchedulingSettings ss)
    {
		ModifiedFixedThreadPool threadPool = new ModifiedFixedThreadPool(3);
        final SA SASchr = new SA();
		
		class StepRunnable implements Runnable
		{
			private int i;
			public StepRunnable(int index)
			{
				i = index;
			}
			@Override
			public void run()
			{
                population[i].calVelocity();
                population[i].updatePosition();
                population[i].updateFitness();
                
                Schedule s = SASchr.getSchedule(ss, population[i]);
                population[i].loadPosition(s);
                population[i].updateFitness();
			}
		}
		
		
        init(ss);
        for (int i = 0; i < POP_SIZE; i++)
        {
            population[i] = new PSOIndividual(ss, globalVars);
            population[i].random();
        }
        for (int k = 0; k < ITERATIONS; k++)
        {
            for (int j = 0; j < POP_SIZE; j++)
            {
				threadPool.submit(new StepRunnable(j));
            }
			threadPool.waitUntilAllTaskFinish();
        }
		threadPool.shutdown();
		
        return (Schedule)globalVars.get(VAR_GBEST);
    }

//    public static void main(String[] args)
//    {
//        Utils.disableDB();
//        Workflow wf = Workflow.fromDummyDAX("/home/orachun/WorkflowEngine/dummy-dags/Inspiral_100.xml.dummy", true);
//        System.out.println(wf.getCumulatedExecTime());
//        ExecSite es = ExecSite.generate(30);
//        
//        //1.7852800115624992
//        Utils.setPropIfNotExist(PROP_ITERATIONS, "200");
//        Utils.setPropIfNotExist(PROP_POP_SIZE, "25");
//        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.3");
//        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.6");
//        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.1");
//        
//        Utils.setPropIfNotExist(PROP_START_TEMP, "50");
//        Utils.setPropIfNotExist(PROP_STOP_TEMP, "1");
//        Utils.setPropIfNotExist(PROP_INNER_COUNT, "2");
//        Utils.setPropIfNotExist(PROP_DECREASE_RATE, "10");
//        
//        Scheduler schr = new PSOSA();
//        SchedulingSettings ss = new SchedulingSettings(wf, es, new CostOptimizationFC(wf.getCumulatedExecTime()*0.1,10,1));
//        
//        double total = 0;
//        for(int i=0;i<10;i++)
//        {
//            Schedule psos = schr.getSchedule(ss);
//            System.out.println(psos.getFitness());
//            System.out.println("\t"+psos.getMakespan());
//            System.out.println("\t"+psos.getCost());
//            total += psos.getFitness();
//        }
//        System.out.println(total/10);
//    }
}
