/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.HashMap;
import removed.ExecSite;
import static workflowengine.schedule.SA.PROP_DECREASE_RATE;
import static workflowengine.schedule.SA.PROP_INNER_COUNT;
import static workflowengine.schedule.SA.PROP_START_TEMP;
import static workflowengine.schedule.SA.PROP_STOP_TEMP;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;

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
    private HashMap<String, Object> globalVars;

    public void init()
    {
        Utils.setPropIfNotExist(PROP_ITERATIONS, "100");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "20");
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.3");
        
        ITERATIONS = Utils.getIntProp(PROP_ITERATIONS);
        POP_SIZE = Utils.getIntProp(PROP_POP_SIZE);
        globalVars = new HashMap<>();
        globalVars.put(PSOSA.VAR_GBEST, null);
        population = new PSOIndividual[POP_SIZE];
    }
    
    public Schedule getSchedule(SchedulerSettings ss)
    {
        init();
        SA SASchr = new SA();
        for (int i = 0; i < POP_SIZE; i++)
        {
            population[i] = new PSOIndividual(ss, globalVars);
            population[i].random();
        }
        for (int k = 0; k < ITERATIONS; k++)
        {
            for (int j = 0; j < POP_SIZE; j++)
            {
                population[j].calVelocity();
                population[j].updatePosition();
                population[j].updateFitness();
                
                
                ss.setParam("init_schedule", population[j]);
                Schedule s = SASchr.getSchedule(ss);
                population[j].loadPosition(s);
                population[j].updateFitness();
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
        
        //1.7852800115624992
        Utils.setPropIfNotExist(PROP_ITERATIONS, "200");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "25");
        Utils.setPropIfNotExist(PROP_PBEST_WEIGHT, "0.3");
        Utils.setPropIfNotExist(PROP_GBEST_WEIGHT, "0.6");
        Utils.setPropIfNotExist(PROP_INERTIA_WEIGHT, "0.1");
        
        Utils.setPropIfNotExist(PROP_START_TEMP, "50");
        Utils.setPropIfNotExist(PROP_STOP_TEMP, "1");
        Utils.setPropIfNotExist(PROP_INNER_COUNT, "2");
        Utils.setPropIfNotExist(PROP_DECREASE_RATE, "10");
        
        Scheduler schr = new PSOSA();
        SchedulerSettings ss = new SchedulerSettings(wf, es, new CostOptimizationFC(wf.getCumulatedExecTime()*0.1,10,1));
        
        double total = 0;
        for(int i=0;i<10;i++)
        {
            Schedule psos = schr.getSchedule(ss);
            System.out.println(psos.getFitness());
            System.out.println("\t"+psos.getMakespan());
            System.out.println("\t"+psos.getCost());
            total += psos.getFitness();
        }
        System.out.println(total/10);
    }
}
