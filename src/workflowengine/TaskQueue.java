/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

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
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFactory;

/**
 *
 * @author orachun
 */
public class TaskQueue
{
	protected Map<Task, String> taskMap = new HashMap<>();
	protected LinkedList<Task> taskQueue = new LinkedList<>();
	
	public void submit(Task t, String target)
	{
		taskMap.put(t, target);
		taskQueue.add(t);
	}
	
	public void submit(Map<Task, String> taskMap)
	{
		taskQueue.addAll(taskMap.keySet());
		this.taskMap.putAll(taskMap);
	}
	
	
	public void submit(String target, Workflow wf)
	{
		Queue<Task> tq = wf.getTaskQueue();
		while(!tq.isEmpty())
		{
			Task t = tq.poll();
			submit(t, target);
		}
	}
	
	public void submit(Schedule s)
	{
		submit(s.getMapping());
	}
	
	public ScheduleEntry poll()
	{
		Task t = taskQueue.poll();
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
		HashMap<String, HashMap<Workflow, List<Task>>> readyTaskMap = new HashMap<>();
		ListIterator<Task> iterator = taskQueue.listIterator();
		Task t;
		while(iterator.hasNext())
		{
			t = iterator.next();
			if(t.isReady())
			{
				String target = taskMap.get(t);
				HashMap<Workflow, List<Task>> workflowReadyTasksMap = readyTaskMap.get(target);
				if(workflowReadyTasksMap == null)
				{
					workflowReadyTasksMap = new HashMap<>();
					readyTaskMap.put(target, workflowReadyTasksMap);
				}
				Workflow wf = WorkflowFactory.get(t.getWfUUID());
				List<Task> readyTaskList = workflowReadyTasksMap.get(wf);
				if(readyTaskList == null)
				{
					readyTaskList = new LinkedList<>();
					workflowReadyTasksMap.put(wf, readyTaskList);
				}
				readyTaskList.add(t);
			}
		}
		
		HashMap<String, Set<Workflow>> readyWorkflow = new HashMap<>();
		for(Map.Entry<String, HashMap<Workflow, List<Task>>> entry: readyTaskMap.entrySet())
		{
			for(Map.Entry<Workflow, List<Task>> workflowEntry : entry.getValue().entrySet())
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
	public String getTargetForTask(Task t)
	{
		return taskMap.get(t);
	}
}


