package workflowengine.schedule;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import workflowengine.server.WorkflowExecutor;
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
//            calCost();
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
        LinkedList<String> finishedTasks = new LinkedList<>();
        estimatedStart.clear();
        estimatedFinish.clear();

//        for (String tid : settings.getFixedTasks())
//        {
//            estimatedStart.put(tid, 0.0);
//            estimatedFinish.put(tid, 0.0);
//        }
//        finishedTasks.addAll(settings.getFixedTasks());
		
		HashMap<String, Set<String>> existingFiles = new HashMap<>();
		for(String worker : settings.getSiteArray())
		{
			existingFiles.put(worker, new HashSet<String>());
		}
		
        makespan = 0;
		cost = 0;
        Queue<String> pendingTasks = settings.getWorkflow().getTaskQueueByOrder();
        while (!pendingTasks.isEmpty())
        {
            String tid = pendingTasks.poll();
            if (finishedTasks.contains(tid))
            {
                continue;
            }
            if (!finishedTasks.containsAll(settings.getWorkflow().getParent(tid)))
            {
                pendingTasks.add(tid);
                continue;
            }

            String worker = this.getWorkerForTask(tid);
			Set<String> fileSet = existingFiles.get(worker);
			Site site = settings.getSite(worker);
			double serverReadyTime = site.getAvailableTime(makespan);
            double parentFinishTime = 0;
            for (String parentTid : settings.getWorkflow().getParent(tid))
            {
                parentFinishTime = Math.max(parentFinishTime, estimatedFinish.get(parentTid));
            }
			
            double taskStartTime = Math.max(parentFinishTime, serverReadyTime);
			
			Task t = Task.get(tid);
			cost += WorkflowExecutor.COST_PER_SECOND * t.getEstimatedExecTime();
			
			//Add input file stage-in time
			for(String fid : t.getInputFiles())
			{
				if(!fileSet.contains(fid))
				{
					double fileSize = WorkflowFile.get(fid).getSize();
					taskStartTime += settings.getExecNetwork().getTransferTime(
						worker, 
						fileSize
						);
					fileSet.add(fid);
					
					//Add input file stage-in cost
					cost += WorkflowExecutor.COST_PER_BYTE * fileSize;
				}
			}
			
			
			
            double taskFinishTime = taskStartTime + t.getEstimatedExecTime();
			
			//Add output file stage-out time
			Set<String> outputFiles = t.getOutputFiles();
			taskFinishTime += settings.getExecNetwork().getTransferTime(worker, outputFiles);
			fileSet.addAll(outputFiles);
			
			//Add output file stage-out cost
			for(String fid : outputFiles)
			{
				cost += WorkflowExecutor.COST_PER_BYTE * WorkflowFile.get(fid).getSize();
			}
			
			site.scheduleJob(taskStartTime, taskFinishTime);
			
            estimatedStart.put(tid, taskStartTime);
            estimatedFinish.put(tid, taskFinishTime);
            makespan = Math.max(makespan, taskFinishTime);
            finishedTasks.add(tid);
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
	
	public void save()
	{
//		for(Map.Entry<String, String>  entry : mapping.entrySet())
//		{
//			String tid = entry.getKey();
//			String wkid = entry.getValue();
//			
//			MongoDB.SCHEDULE.update(new BasicDBObject("tid", tid), 
//					new BasicDBObject("tid", tid)
//					.append("wkid", wkid)
//					.append("estimated_start", estimatedStart.get(tid))
//					.append("estimated_finish", estimatedFinish.get(tid))
//					.append("wfid", settings.getWorkflow().getSuperWfid())
//					, true, false);
//		}
		ScheduleTable scht = getScheduleTable();
		scht.cache();
		scht.save();
	}
	
	public String getWorkflowID()
	{
		return settings.getWorkflow().getUUID();
	}
	
	public ScheduleTable getScheduleTable()
	{
		ScheduleTable scht = new ScheduleTable(settings.getWorkflow().getSuperWfid(), settings.getWorkflow().getUUID(), mapping, estimatedStart, estimatedFinish);
		scht.setCost(cost);
		scht.setMakespan(makespan);
		return scht;
	}
}

















