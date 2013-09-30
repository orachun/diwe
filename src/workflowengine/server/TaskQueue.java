/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.utils.db.Cacher;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class TaskQueue
{
	protected Map<String, String> taskMap = new HashMap<>(); //TaskUUID - Worker URI
	protected LinkedList<String> taskQueue = new LinkedList<>(); //Task UUID
	
	public void submit(String t, String target)
	{
		taskMap.put(t, target);
		taskQueue.add(t);
	}
	
	public void submit(Map<String, String> taskMap)
	{
		taskQueue.addAll(taskMap.keySet());
		this.taskMap.putAll(taskMap);
	}
	
	
	public void submit(String target, Workflow wf)
	{
		Queue<String> tq = wf.getTaskQueue();
		while(!tq.isEmpty())
		{
			submit(tq.poll(), target);
		}
	}
	
	public void submit(Schedule s)
	{
		submit(s.getMapping());
	}
	
	public ScheduleEntry poll()
	{
		String t = taskQueue.poll();
		if(t == null)
		{
			return null;
		}
		return new ScheduleEntry(t, taskMap.remove(t));
	}
	
	public boolean isEmpty()
	{
		return taskQueue.isEmpty();
	}
	
	/**
	 * Poll ready tasks as sub-workflows for each target
	 * @return a map between a target string and a set of sub-workflows
	 */
	public Map<String, Set<Workflow>> pollNextReadyTasks()
	{
		HashMap<String, HashMap<Workflow, List<String>>> readyTaskMap = new HashMap<>();
		ListIterator<String> iterator = taskQueue.listIterator();
		Task t;
		String taskUUID;
		while(iterator.hasNext())
		{
			taskUUID = iterator.next();
			t = Task.get(taskUUID);
			if(t.isReady())
			{
				String target = taskMap.get(taskUUID);
				HashMap<Workflow, List<String>> workflowReadyTasksMap = readyTaskMap.get(target);
				if(workflowReadyTasksMap == null)
				{
					workflowReadyTasksMap = new HashMap<>();
					readyTaskMap.put(target, workflowReadyTasksMap);
				}
				Workflow wf = (Workflow)Cacher.get(Workflow.class, t.getWfUUID());
				List<String> readyTaskList = workflowReadyTasksMap.get(wf);
				if(readyTaskList == null)
				{
					readyTaskList = new LinkedList<>();
					workflowReadyTasksMap.put(wf, readyTaskList);
				}
				readyTaskList.add(taskUUID);
			}
		}
		
		HashMap<String, Set<Workflow>> readyWorkflow = new HashMap<>();
		for(Map.Entry<String, HashMap<Workflow, List<String>>> entry: readyTaskMap.entrySet())
		{
			for(Map.Entry<Workflow, List<String>> workflowEntry : entry.getValue().entrySet())
			{
				Workflow oriWf = workflowEntry.getKey();
				Workflow subWf = oriWf.getSubworkflow(oriWf.getName(), workflowEntry.getValue());
				Set<Workflow> wfSet = readyWorkflow.get(entry.getKey());
				if(wfSet == null)
				{
					wfSet = new HashSet<>();
					readyWorkflow.put(entry.getKey(), wfSet);
				}
				wfSet.add(subWf);
			}
		}
		return readyWorkflow;
	}
	public String getTargetForTask(String taskUUID)
	{
		return taskMap.get(taskUUID);
	}
}


