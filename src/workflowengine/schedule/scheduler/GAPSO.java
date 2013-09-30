/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.scheduler.Scheduler;
import java.util.ArrayList;
import java.util.Collections;
import removed.ExecSite;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleComparator;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class GAPSO extends GA
{
    public static final String PROP_KT = "gapso_kt_percent";
    public static final String PROP_KR = "gapso_kr_percent";
    
    private double kt;
    private double kr;
    public void init(SchedulingSettings settings)
    {
        super.init(settings);
        kt = Utils.getDoubleProp(PROP_KT);
        kr = Utils.getDoubleProp(PROP_KR);
    }
    public Schedule getSchedule(SchedulingSettings settings)
    {
        init(settings);
        population = new ArrayList<>();
        
        Schedule HEFTs = new HEFT().getSchedule(settings);
        GAIndividual HEFTInd = new GAIndividual(HEFTs);
        population.add(HEFTInd);
        for (int i = 0; i < POP_SIZE; i++)
        {
            GAIndividual ind = new GAIndividual(settings);
            ind.random(settings.getTotalWorkers());
            population.add(ind);
        }
        Collections.sort(population, new ScheduleComparator());
        
        
        for (int i = 0; i < ITERATION; i++)
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
            Collections.sort(newPopulation, new ScheduleComparator());
            for (int k = 0; k < POP_SIZE*3/4; k++)
            {
//                System.out.println(k+"   "+population.get(k).getFitness());
                settings.setParam("old_solution", newPopulation.get(k));
                PSO scheduler = new PSO();
                int a = r.nextInt(settings.getTotalTasks());
                int b = a + (int)Math.floor(kt*settings.getTotalTasks());
                b = Math.min(b, settings.getTotalTasks());
                
                int d = (int)Math.floor(kr * settings.getTotalWorkers());
                d = Math.min(d, settings.getTotalWorkers());
                GAIndividual ind = new GAIndividual(scheduler.limitBound(b, a, d, settings));
                newPopulation.add(ind);
//                if(ind.getFitness() < 475.86)
//                {
//                    System.out.println(k+"   "+ind.toString());
//                    System.out.println(k+"   "+ind.getFitness());
//                }
            }
            newPopulation.addAll(population);
            Collections.sort(newPopulation, new ScheduleComparator());
            population = newPopulation.subList(0, POP_SIZE);
//            System.out.println(population.get(0).getFitness());
        }
        return population.get(0);
    }
    
    public static void main(String[] args)
    {
        Utils.disableDB();
        Workflow wf = Workflow.fromDAX("/home/orachun/Desktop/dag.xml", true);
        ExecSite es = ExecSite.generate(30);
        
        
        Utils.setPropIfNotExist(PROP_ITERATIONS, "20");
        Utils.setPropIfNotExist(PROP_POP_SIZE, "5");
        Utils.setPropIfNotExist(PSO.PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PSO.PROP_GBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PSO.PROP_INERTIA_WEIGHT, "0.2");
        
        Scheduler GASch = new GAPSO();
        Scheduler HEFTSch = new HEFT();
        SchedulingSettings ss = new SchedulingSettings(wf, es, new CostOptimizationFC());
        Schedule HEFTs = HEFTSch.getSchedule(ss);
        System.out.println(HEFTs.getFitness());
        System.out.println(HEFTs.getMakespan());
        System.out.println(HEFTs.getCost());
        Schedule GAs = GASch.getSchedule(ss);
        System.out.println(GAs.getFitness());
        System.out.println(GAs.getMakespan());
        System.out.println(GAs.getCost());
    }
}
