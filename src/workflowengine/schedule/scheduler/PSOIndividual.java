/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import java.util.HashMap;
import java.util.Random;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.utils.UniqueMap;
import workflowengine.utils.Utils;

/**
 *
 * @author Dew
 */
public class PSOIndividual extends Schedule
{
    private int[] position;
    private double[] velocity;
    private PSOIndividual pBest;
    private Random r = new Random();
    private static double PBEST_WEIGHT;
    private static double GBEST_WEIGHT;
    private static double INERTIA_WEIGHT;
    private HashMap<String, Object> globalVars;
    private int startTask;
    private int endTask;
    
    private Schedule oldSch;

    public PSOIndividual(SchedulingSettings settings, HashMap<String, Object> globalVars)
    {
        super(settings);
        this.globalVars = globalVars;
        position = new int[settings.getTotalTasks()];
        velocity = new double[settings.getTotalTasks()];
        pBest = null;
        if(settings.hasParam("taskLowerBound"))
        {
            startTask = settings.getIntParam("taskLowerBound");
            endTask = settings.getIntParam("taskUpperBound");
        }
        else
        {
            startTask = 0;
            endTask = settings.getTotalTasks();
        }
        PBEST_WEIGHT = Utils.getDoubleProp(PSO.PROP_PBEST_WEIGHT);
        GBEST_WEIGHT = Utils.getDoubleProp(PSO.PROP_GBEST_WEIGHT);
        INERTIA_WEIGHT = Utils.getDoubleProp(PSO.PROP_INERTIA_WEIGHT);
        oldSch = (Schedule) settings.getObjectParam("old_solution");
    }
    
    public PSOIndividual(PSOIndividual p)
    {
        super(p);
        this.globalVars = p.globalVars;
        position = new int[settings.getTotalTasks()];
        velocity = new double[settings.getTotalTasks()];
        System.arraycopy(p.position, 0, this.position, 0, this.position.length);
        System.arraycopy(p.velocity, 0, this.velocity, 0, this.velocity.length);
        this.pBest = p.pBest;
        this.startTask = p.startTask;
        this.endTask = p.endTask;
    }

    public PSOIndividual(Schedule s, HashMap<String, Object> globalVars)
    {
        super(s);
        this.globalVars = globalVars;
        position = new int[settings.getTotalTasks()];
        velocity = new double[settings.getTotalTasks()];
        pBest = null;
        if(settings.hasParam("taskLowerBound"))
        {
            startTask = settings.getIntParam("taskLowerBound");
            endTask = settings.getIntParam("taskUpperBound");
        }
        else
        {
            startTask = 0;
            endTask = settings.getTotalTasks();
        }
        PBEST_WEIGHT = Utils.getDoubleProp(PSO.PROP_PBEST_WEIGHT);
        GBEST_WEIGHT = Utils.getDoubleProp(PSO.PROP_GBEST_WEIGHT);
        INERTIA_WEIGHT = Utils.getDoubleProp(PSO.PROP_INERTIA_WEIGHT);
        oldSch = (Schedule) settings.getObjectParam("old_solution");
    }
    
    void calVelocity()
    {
        final PSOIndividual gBest = (PSOIndividual)globalVars.get(PSO.VAR_GBEST);
		synchronized(gBest)
		{
			for (int i = startTask; i < endTask; i++)
			{
				velocity[i] = INERTIA_WEIGHT * velocity[i] 
						+ (PBEST_WEIGHT * r.nextDouble() * (pBest.position[i] - position[i])) 
						+ (GBEST_WEIGHT * r.nextDouble() * (gBest.position[i] - position[i]));
			}
		}
    }

    void updatePosition()
    {
        for (int i = startTask; i < endTask; i++)
        {
            setPosition(i, position[i] + velocity[i]);
        }
    }
    
    private void setPosition(int i, double val)
    {
		UniqueMap<Integer, String> workerMap = (UniqueMap<Integer, String>)
				settings.getObjectParam("workerMap");
		String[] workers = (String[])settings.getObjectParam("workers");
		String[] tasks = (String[])settings.getObjectParam("tasks");
		
        double newPosition = val;
        int workerID = (int) Math.abs(Math.round(newPosition)) % settings.getTotalWorkers();
        if(settings.hasParam("taskLowerBound"))
        {
            int oldWorkerID = workerMap.getKey(oldSch.getWorkerForTask(tasks[i]));
            workerID = (int) Math.floor(workerID + oldWorkerID - settings.getIntParam("resourceBoundLength") / 2.0);
            workerID = Math.max(0, workerID);
            workerID = Math.min(settings.getTotalWorkers()-1, workerID);
        }
        position[i] = workerID;
		
		
        this.setWorkerForTask(tasks[i], workers[position[i]]);
    }
    
    public void loadPosition()
    {
        loadPosition(this);
    }
    public void loadPosition(Schedule s)
    {
		UniqueMap<Integer, String> workerMap = (UniqueMap<Integer, String>)
				settings.getObjectParam("workerMap");
		String[] tasks = (String[])settings.getObjectParam("tasks");
        for(int i=0;i<settings.getTotalTasks();i++)
        {
            position[i] = workerMap.getKey(s.getWorkerForTask(tasks[i]));
        }
        updatePosition();
    }

    void updateFitness()
    {
//        Schedule sch = (Schedule) settings.getObjectParam("old_solution");
//        if(settings.hasParam("taskLowerBound"))
//        {
//            int totalworker = settings.getIntParam("resourceBoundLength");
//            for (int i = 0; i < settings.getTotalTasks(); i++)
//            {
//                if(i >= startTask && i < endTask)
//                {
//                    int workerID = (int) Math.abs(Math.floor(position[i])) % totalworker;         
//                    int oldWorkerID = settings.getWorkerIndex(sch.getWorkerForTask(i));
//                    workerID = (int) Math.floor(workerID + oldWorkerID - settings.getIntParam("resourceBoundLength") / 2.0);
//                    workerID = Math.max(0, workerID);
//                    workerID = Math.min(settings.getTotalWorkers()-1, workerID);
//
//                    this.setWorkerForTask(i, settings.getWorker(workerID));
//                }
//                else
//                {
//                    this.setWorkerForTask(i, sch.getWorkerForTask(i));
//                }
//            }
//        }
//        else
//        {
//            int totalworker = settings.getTotalWorkers();
//            for (int i = 0; i < settings.getTotalTasks(); i++)
//            {
//                int workerID = (int) Math.abs(Math.floor(position[i])) % totalworker;  
//                this.setWorkerForTask(i, settings.getWorker(workerID));
//            }
//        }
        
        this.pBest = new PSOIndividual((PSOIndividual)settings.getFc().getBetterSchedule(this, this.pBest));
        
        PSOIndividual gBest = (PSOIndividual)globalVars.get(PSO.VAR_GBEST);
        gBest = new PSOIndividual((PSOIndividual)settings.getFc().getBetterSchedule(this, gBest));
        globalVars.put(PSO.VAR_GBEST, gBest);
    }

    @Override
    public void random()
    {
		String[] workers = (String[])settings.getObjectParam("workers");
		String[] tasks = (String[])settings.getObjectParam("tasks");
        for (int i = startTask; i < endTask; i++)
        {
            position[i] = r.nextInt(settings.getTotalWorkers());
            velocity[i] = r.nextDouble() * settings.getTotalWorkers();
            this.setWorkerForTask(tasks[i], workers[position[i]]);
        }
        updateFitness();
    }
    
    public void setZero()
    {
		String[] workers = (String[])settings.getObjectParam("workers");
		String[] tasks = (String[])settings.getObjectParam("tasks");
        for (int i = startTask; i < endTask; i++)
        {
            position[i] = 0;
            velocity[i] = r.nextDouble() * settings.getTotalWorkers();
            this.setWorkerForTask(tasks[i], workers[0]);
        }
        updateFitness();
    }
}
