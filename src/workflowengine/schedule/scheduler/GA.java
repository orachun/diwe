package workflowengine.schedule.scheduler;

import workflowengine.schedule.SchedulingSettings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleComparator;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;


/**
 *
 * @author Dew
 */
public class GA implements Scheduler
{
    public static final String PROP_ITERATIONS = "ga_iterations";
    public static final String PROP_ELITISM = "ga_elitism_percent";
    public static final String PROP_POP_SIZE = "ga_pop_size";
    public static final String PROP_MUTATE_RATE = "ga_mutate_rate";

    protected static double ITERATION;
    protected static double ELITISM;
    protected static int POP_SIZE;
    protected static double MUTATE_RATE;
    protected List<GAIndividual> population;
    protected static Random r = new Random();
    private ScheduleComparator scp = new ScheduleComparator();
    private SchedulingSettings settings;
    
    
    public List<GAIndividual> getPop()
    {
        return population;
    }
    
    GAIndividual rouletWheel()
    {
        double total = 0;
        for (int j = 0; j < POP_SIZE; j++)
        {
            total = total + (1.0 / (population.get(j).getFitness()));
        }


        double sum = 0;
        double rand = r.nextDouble();
        for (int i = 0; i < POP_SIZE; i++)
        {
            sum = sum + (1.0 / (population.get(i).getFitness()) / total);
            if (rand < sum)
            {
                return population.get(i);
            }
        }
        return null;
    }

    
    public void init(SchedulingSettings ss)
    {
        this.settings = ss;
        ITERATION = Utils.getIntProp(PROP_ITERATIONS);
        ELITISM = Utils.getDoubleProp(PROP_ELITISM);
        POP_SIZE = Utils.getIntProp(PROP_POP_SIZE);
        MUTATE_RATE = Utils.getDoubleProp(PROP_MUTATE_RATE);
        
        population = new ArrayList<>();
        population.add(new GAIndividual(new HEFT().getSchedule(settings)));
        for (int i = 0; i < POP_SIZE; i++)
        {
            GAIndividual ind = new GAIndividual(settings);
            ind.random(settings.getTotalWorkers());
            population.add(ind);
        }
        Collections.sort(population, new ScheduleComparator());
    }
    
    public void step()
    {
        ArrayList<GAIndividual> newPopulation = new ArrayList<>();
            
        for (int j = 0; j < POP_SIZE * ELITISM; j++)
        {
            newPopulation.add(population.get(j));
        }
        for (int k = 0; k < (POP_SIZE - POP_SIZE * ELITISM) / 2; k++)
        {
            GAIndividual p1 = rouletWheel();
            GAIndividual p2 = rouletWheel();
            GAIndividual[] childs = p1.crossover(p2);
            if (r.nextDouble() < MUTATE_RATE)
            {
                childs[0].mutation();
                childs[1].mutation();
            }
            newPopulation.add(childs[0]);
            newPopulation.add(childs[1]);
        }

        GAIndividual ind = new GAIndividual(settings);
        ind.random(settings.getTotalWorkers());
        population.add(ind);

        Collections.sort(newPopulation, scp);
        population = newPopulation;
    }
    
    public void sort()
    {
        Collections.sort(population, scp);
    }
    
    public Schedule getSchedule(SchedulingSettings settings)
    {
        this.settings = settings;
        init(settings);
        
        for (int i = 0; i < ITERATION; i++)
        {
            step();
        }
        return scp.getBest();
    }
    
    public Schedule getBestSchedule()
    {
        return scp.getBest();
    }
    
   public static void main(String[] args)
    {
        Utils.disableDB();
        Workflow wf = Workflow.fromDummyDAX("/home/orachun/WorkflowEngine/dummy-dags/Inspiral_100.xml.dummy", true);
        System.out.println(wf.getCumulatedExecTime());
        ExecSite es = ExecSite.generate(30);
        
        
        //1.7770173962500013
        Utils.setPropIfNotExist(PROP_ITERATIONS, "200");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "25");
        Utils.setPropIfNotExist(PROP_ELITISM, "0.2");
        Utils.setPropIfNotExist(PROP_MUTATE_RATE, "0.8");
        
        //1.5847634568749998
        Utils.setPropIfNotExist(PROP_ELITISM, "0.1");
        Utils.setPropIfNotExist(PROP_MUTATE_RATE, "0.8");
        
        //1.7448683021874998
        Utils.setPropIfNotExist(PROP_ELITISM, "0.2");
        Utils.setPropIfNotExist(PROP_MUTATE_RATE, "0.5");
        
        Scheduler schr = new GA();
        SchedulingSettings ss = new SchedulingSettings(wf, es, new CostOptimizationFC(wf.getCumulatedExecTime()*0.1,10,1));
        
        double total = 0;
        for(int i=0;i<10;i++)
        {
            Schedule s = schr.getSchedule(ss);
            System.out.println(s.getFitness());
            System.out.println("\t"+s.getMakespan());
            System.out.println("\t"+s.getCost());
            double fit = s.getFitness();
            total+=fit;
        }
        System.out.println(total/10);
    }
}
