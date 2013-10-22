package workflowengine.schedule;

import com.mongodb.BasicDBObject;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.MongoDB;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author Orachun
 */
public class Schedule
{
    protected final SchedulingSettings settings;
    private HashMap<String, String> mapping; //Mapping from taskUUID to worker URI
    private HashMap<String, Double> estimatedStart;
    private HashMap<String, Double> estimatedFinish;
    private double makespan;
    private double cost;
    private boolean edited;
    private double fitness;

    public Schedule(SchedulingSettings settings)
    {
        this.settings = settings;
        estimatedStart = new HashMap<>(settings.getTotalTasks()); 
        estimatedFinish = new HashMap<>(settings.getTotalTasks()); 
        mapping = new HashMap<>(settings.getTotalTasks());
        mapping.putAll(settings.getFixedMapping());
        mapAllTaskToFirstWorker();
        edited = true;
    }

    public SchedulingSettings getSettings()
    {
        return settings;
    }

	public Map<String, String> getMapping()
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
        for (String taskUUID : settings.getTaskUUIDSet())
        {
            setWorkerForTask(taskUUID, w);
        }
        this.edited = true;
    }

    public String getWorkerForTask(String taskUUID)
    {
        return mapping.get(taskUUID);
    }

    public void setWorkerForTask(String taskUUID, String worker)
    {
        if (!settings.isFixedTask(taskUUID))
        {
            mapping.put(taskUUID, worker);
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
//		HashMap<String, Double> workerReadyTime = new HashMap<>(
//				settings.getExecNetwork().getExecutorURISet().size());
        LinkedList<String> finishedTasks = new LinkedList<>();
        estimatedStart.clear();
        estimatedFinish.clear();

        for (String taskUUID : settings.getFixedTasks())
        {
            estimatedStart.put(taskUUID, 0.0);
            estimatedFinish.put(taskUUID, 0.0);
        }

        finishedTasks.addAll(settings.getFixedTasks());
//		for(String worker : settings.getExecNetwork().getExecutorURISet())
//		{
//			workerReadyTime.put(worker, 0.0);
//		}
		
        makespan = 0;
        Queue<String> pendingTasks = settings.getWorkflow().getTaskQueue();
        while (!pendingTasks.isEmpty())
        {
            String taskUUID = pendingTasks.poll();
            if (finishedTasks.contains(taskUUID))
            {
                continue;
            }
            if (!finishedTasks.containsAll(settings.getWorkflow().getParent(taskUUID)))
            {
                pendingTasks.add(taskUUID);
                continue;
            }

            String worker = this.getWorkerForTask(taskUUID);
			Site site = settings.getSite(worker);
//            double serverReadyTime = workerReadyTime.get(worker);
			double serverReadyTime = site.getAvailableTime(makespan);
            double parentFinishTime = 0;
            for (String parentTaskUUID : settings.getWorkflow().getParent(taskUUID))
            {
                parentFinishTime = Math.max(parentFinishTime, estimatedFinish.get(parentTaskUUID));
            }
            double taskStartTime = Math.max(parentFinishTime, serverReadyTime);
            double taskFinishTime = taskStartTime + Task.get(taskUUID).getEstimatedExecTime();
            
            for (String childTaskUUID : settings.getWorkflow().getChild(taskUUID))
            {
                String workerForC = this.getWorkerForTask(childTaskUUID);
                if(!worker.equals(workerForC))
                {
					for(String wff : Task.get(taskUUID).getOutputFileUUIDsForTask(childTaskUUID))
					{
						WorkflowFile f = (WorkflowFile)Cacher.get(WorkflowFile.class, wff);
						taskFinishTime += settings.getExecNetwork()
								.getTransferTime(worker, workerForC, f.getSize());
					}
                }
            }
            
			site.scheduleJob(taskStartTime, taskFinishTime);
			
            estimatedStart.put(taskUUID, taskStartTime);
            estimatedFinish.put(taskUUID, taskFinishTime);
//			workerReadyTime.put(worker, taskFinishTime);
            makespan = Math.max(makespan, taskFinishTime);
            finishedTasks.add(taskUUID);
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
        for (String taskUUID : mapping.keySet())
        {
//            System.out.println(t+"->"+mapping.get(t)+ " start: "+ t.getDoubleProp("startTime")+ " end: "+t.getDoubleProp("finishTime"));
            System.out.println(taskUUID + "->" + mapping.get(taskUUID) 
                    + " start: " + estimatedStart.get(taskUUID) 
                    + " end: " + estimatedFinish.get(taskUUID));
        }
    }
    
    public double getEstimatedStart(String t)
    {
        return estimatedStart.get(t);
    }
    
    public double getEstimatedFinish(String t)
    {
        return estimatedFinish.get(t);
    }
    
	@Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for(String taskUUID : settings.getTaskUUIDSet())
        {
            sb.append(taskUUID).append(":").append(this.getWorkerForTask(taskUUID)).append(", ");
        }
        return sb.toString();
    }
	
	private static String[] keys = new String[]{"tid", "wkid"};
	public void save()
	{
		for(Map.Entry<String, String>  entry : mapping.entrySet())
		{
			String tid = entry.getKey();
			String wkid = entry.getValue();
//			new DBRecord("schedule")
//					.set("tid", tid)
//					.set("wkid", wkid)
////					.set("estimated_start", estimatedStart.get(tid))
////					.set("estimated_finish", estimatedFinish.get(tid))
//					.upsert(keys);
			
			MongoDB.SCHEDULE.update(new BasicDBObject("tid", tid), 
					new BasicDBObject("tid", tid)
					.append("wkid", wkid)
					.append("estimated_start", estimatedStart.get(tid))
					.append("estimated_finish", estimatedFinish.get(tid))
					, true, false);
		}
	}
	
	public String getWorkflowID()
	{
		return settings.getWorkflow().getUUID();
	}
	
	
}

















