/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.List;
import removed.ExecSite;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class GAPSO1 implements Scheduler
{
    public static final String PROP_ITERATIONS = "gapso_iterations";
    protected static double ITERATION;
    
    GA ga = new GA();
    PSO pso = new PSO();
    public void init()
    {
        ITERATION = Utils.getIntProp(PROP_ITERATIONS);
    }
    
    @Override
    public Schedule getSchedule(SchedulerSettings settings)
    {
        ITERATION = Utils.getIntProp(PROP_ITERATIONS);
        ga.init(settings);
        pso.init(settings);
        
        Schedule heftSchedule = new HEFTScheduler().getSchedule(settings);
        List<GAIndividual> gapop = ga.getPop();
        List<PSOIndividual> psopop = pso.getPop();

        gapop.set(gapop.size()-1, new GAIndividual(heftSchedule));
        psopop.get(psopop.size()-1).loadPosition(heftSchedule);
        
        
        
        ga.getPop().set(0, new GAIndividual(heftSchedule));
        
        for(int i=0;i<ITERATION;i++)
        {
            ga.step();
            pso.step();
            pso.sort();
            
            gapop = ga.getPop();
            psopop = pso.getPop();
            
            for(int a=0;a<gapop.size()*0.2;a++)
            {
                gapop.set(gapop.size()-1-a, new GAIndividual(psopop.get(a)));
                psopop.get(psopop.size()-1-a).loadPosition(gapop.get(a));
            }
            
        }
        
        Schedule spso = pso.getBestSchedule();
        Schedule sga = ga.getBestSchedule();
        if(spso.getFitness() > sga.getFitness())
        {
            return sga;
        }
        else
        {
            return spso;
        }
    }
    public static void main(String[] args)
    {
        Utils.disableDB();
        Workflow wf = Workflow.fromDummyDAX("/home/orachun/WorkflowEngine/dummy-dags/Inspiral_1000.xml.dummy", true);
        System.out.println(wf.getCumulatedExecTime());
        ExecSite es = ExecSite.generate(30);
        //0.2799029074999998
        //0.2799029074999998
        //0.2799029074999998
        
        Utils.setPropIfNotExist(PROP_ITERATIONS, "200");
        
        //1.7770173962500013
        Utils.setPropIfNotExist(GA.PROP_ITERATIONS, "200");
        Utils.setPropIfNotExist(GA.PROP_POP_SIZE, "50");
        Utils.setPropIfNotExist(GA.PROP_ELITISM, "0.1");
        Utils.setPropIfNotExist(GA.PROP_MUTATE_RATE, "0.8");
        
        Utils.setPropIfNotExist(PSO.PROP_ITERATIONS, "200");
        Utils.setPropIfNotExist(PSO.PROP_POP_SIZE, "50");
        //1.8252815646875014
        Utils.setPropIfNotExist(PSO.PROP_PBEST_WEIGHT, "0.4");
        Utils.setPropIfNotExist(PSO.PROP_GBEST_WEIGHT, "0.5");
        Utils.setPropIfNotExist(PSO.PROP_INERTIA_WEIGHT, "0.1");
        
        Scheduler schr = new GA();
        SchedulerSettings ss = new SchedulerSettings(wf, es, new CostOptimizationFC(wf.getCumulatedExecTime()*0.1,10,1));
        
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
