package workflowengine.schedule;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import removed.Worker;
import workflowengine.workflow.Task;

/**
 *
 * @author Orachun
 */
public class Schedule
{
    protected final SchedulerSettings settings;
    private HashMap<Task, String> mapping; //Mapping from task to worker URI
    private HashMap<Task, Double> estimatedStart;
    private HashMap<Task, Double> estimatedFinish;
    private double makespan;
    private double cost;
    private boolean edited;
    private double fitness;

    public Schedule(SchedulerSettings settings)
    {
        this.settings = settings;
        estimatedStart = new HashMap<>(settings.getTotalTasks()); 
        estimatedFinish = new HashMap<>(settings.getTotalTasks()); 
        mapping = new HashMap<>(settings.getTotalTasks());
        mapping.putAll(settings.getFixedMapping());
        mapAllTaskToFirstWorker();
        edited = true;
    }

    public SchedulerSettings getSettings()
    {
        return settings;
    }

	public Map<Task, String> getMapping()
	{
		return Collections.unmodifiableMap(mapping);
	}
	
    //Copy constructor
    protected Schedule(Schedule sch)
    {
//        this.r = sch.r;
//        this.wf = sch.wf;
//        this.es = sch.es;
        mapping = new HashMap<>(sch.mapping);
        this.makespan = sch.makespan;
        this.cost = sch.cost;
        this.edited = sch.edited;
        this.settings = sch.settings;
        estimatedStart = new HashMap<>(sch.estimatedStart); 
        estimatedFinish = new HashMap<>(sch.estimatedFinish); 
        this.fitness = sch.fitness;
//        this.fixedMapping = sch.fixedMapping;
//        taskList = sch.taskList;
    }

    public Schedule copy()
    {
        return new Schedule(this);
    }

    //Map all tasks to the first worker
    private void mapAllTaskToFirstWorker()
    {
		String w = settings.getExecNetwork().getExecutorURISet().iterator().next();
        for (Task t : settings.getTaskSet())
        {
            setWorkerForTask(t, w);
        }
        this.edited = true;
    }

    public String getWorkerForTask(Task t)
    {
        return mapping.get(t);
    }

    public void setWorkerForTask(Task t, String worker)
    {
        if (!settings.isFixedTask(t))
        {
            mapping.put(t, worker);
            edited = true;
        }
    }

    public double getMakespan()
    {
        if (edited)
        {
            evaluate();
        }
        return makespan;

    }

    public double getCost()
    {
        if (edited)
        {
            evaluate();
        }
        return cost;
    }

    public void evaluate()
    {
        if(edited)
        {
            calMakespan();
            calCost();
        }
        edited = false;
        this.fitness = this.settings.getFc().getFitness(this);
    }
    
    public double getFitness()
    {
        if(edited)
        {
            evaluate();
        }
        return fitness;
    }

    private void calMakespan()
    {
		//TODO: modify this to support multiple processors per worker
		HashMap<String, Double> workerReadyTime = new HashMap<>(
				settings.getExecNetwork().getExecutorURISet().size());
        LinkedList<Task> finishedTasks = new LinkedList<>();
        estimatedStart.clear();
        estimatedFinish.clear();

        for (Task t : settings.getFixedTasks())
        {
            estimatedStart.put(t, 0.0);
            estimatedFinish.put(t, 0.0);
        }

        finishedTasks.addAll(settings.getFixedTasks());
		for(String worker : settings.getExecNetwork().getExecutorURISet())
		{
			workerReadyTime.put(worker, 0.0);
		}
		
        makespan = 0;
        LinkedList<Task> pendingTasks = settings.getWorkflow().getTaskQueue();
        while (!pendingTasks.isEmpty())
        {
            Task t = pendingTasks.poll();
            if (finishedTasks.contains(t))
            {
                continue;
            }
            if (!finishedTasks.containsAll(settings.getWorkflow().getParent(t)))
            {
                pendingTasks.push(t);
                continue;
            }

            String worker = this.getWorkerForTask(t);
            double serverReadyTime = workerReadyTime.get(worker);
            double parentFinishTime = 0;
            for (Task p : settings.getWorkflow().getParent(t))
            {
                parentFinishTime = Math.max(parentFinishTime, estimatedFinish.get(p));
            }
            double taskStartTime = Math.max(parentFinishTime, serverReadyTime);
            double taskFinishTime = taskStartTime + t.getEstimatedExecTime();
            
            for (Task c : settings.getWorkflow().getChild(t))
            {
                String workerForC = this.getWorkerForTask(c);
                if(!worker.equals(workerForC))
                {
					taskFinishTime += settings.getExecNetwork()
							.getTransferTime(worker, workerForC, t.getOutputFilesForTask(c));
                }
            }
            
            estimatedStart.put(t, taskStartTime);
            estimatedFinish.put(t, taskFinishTime);
			workerReadyTime.put(worker, taskFinishTime);
            makespan = Math.max(makespan, taskFinishTime);
            finishedTasks.add(t);
        }
    }

    private void calCost()
    {
        //Calculate cost of schedule
        cost = 0;
//        
//        //Exec cost
//        HashMap<String, Double> execTime = new HashMap<>();
//        for (Task t : mapping.keySet())
//        {
//            String w = mapping.get(t);
//            Double time = execTime.get(w);
//            if(time == null)
//            {
//                time = 0.0;
//            }
//            execTime.put(w, time+t.getEstimatedExecTime());
//        }
//        for(String w : execTime.keySet())
//        {
//            double time = execTime.get(w);
////            double remain = time % 60;
////            if(remain > 0)
////            {
////                time += (60 - remain);
////            }
//            cost += time * w.getUnitCost();
//        }
//        
//        
//        //Communication cost
//        LinkedList<Task> queue = settings.getOrderedTaskQueue();
//        while(!queue.isEmpty())
//        {
//            Task t = queue.poll();
//            Worker wt = this.getWorkerForTask(t);
//            cost += settings.getTransferCost(t.getOutputFiles());
//            cost += settings.getTransferCost(t.getInputFiles());
////            for(Task c : settings.getChildTasks(t))
////            {
////                Worker wc = this.getWorkerForTask(c);
////                if(!wt.equals(wc))
////                {
////                    cost += settings.getTransferCost(t.getOutputFilesForTask(c));
////                }
////            }
//        }
//        
////        cost = makespan*settings.getTotalWorkers();
    }

    public void print()
    {
        System.out.println(getMakespan() + " " + getCost());
        for (Task t : mapping.keySet())
        {
//            System.out.println(t+"->"+mapping.get(t)+ " start: "+ t.getDoubleProp("startTime")+ " end: "+t.getDoubleProp("finishTime"));
            System.out.println(t + "->" + mapping.get(t) 
                    + " start: " + estimatedStart.get(t) 
                    + " end: " + estimatedFinish.get(t));
        }
    }
    
    public double getEstimatedStart(Task t)
    {
        return estimatedStart.get(t);
    }
    
    public double getEstimatedFinish(Task t)
    {
        return estimatedFinish.get(t);
    }
    
	@Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for(Task t : settings.getTaskSet())
        {
            sb.append(t.getUUID()).append(":").append(this.getWorkerForTask(t)).append(", ");
        }
        return sb.toString();
    }
}
