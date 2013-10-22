/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class TaskQueue implements Serializable
{ 
	private static final int WORKFLOW_ID = 0;
	private static final int WORKER_URI = 1;
	
	//Task ID -> { WORKFLOW_ID, WORKER_URI }
	protected Map<String, String[]> taskMap = new ConcurrentHashMap<>(); 
	
	protected LinkedList<String> taskQueue = new LinkedList<>(); //Task UUID
		
	public synchronized void submit(Schedule s)
	{
		taskQueue.addAll(s.getSettings().getWorkflow().getTaskQueue());
		for (Map.Entry<String, String> entry : s.getMapping().entrySet())
		{
			this.taskMap.put(entry.getKey(), new String[]
			{
				s.getWorkflowID(), entry.getValue()
			});
		}
	}
	
	/**
	 * Poll only single ready task to be executed
	 * @return 
	 */
	public synchronized ScheduleEntry poll()
	{
		String t = taskQueue.poll();
		if(t == null)
		{
			return null;
		}
		String[] s = taskMap.remove(t);
		String wfDir = Workflow.get(s[WORKFLOW_ID]).getSuperWfid();
		return new ScheduleEntry(t, s[WORKER_URI], wfDir);
	}
	
	
	public boolean isEmpty()
	{
		return taskQueue.isEmpty();
	}
	
	/**
	 * Poll ready tasks as sub-workflows for each target worker
	 * @return a map between a target string and a set of sub-workflows
	 */
	public synchronized Map<String, Set<Workflow>> pollNextReadyTasks()
	{
		//Site -> (Workflow, Task Set)		
		HashMap<String, HashMap<Workflow, Set<String>>> readyTaskMap = new HashMap<>();
		ListIterator<String> iterator = taskQueue.listIterator();
		
		String taskID;
		Set<String> readyTasks = new HashSet<>();
		while(iterator.hasNext())
		{
			taskID = iterator.next();
			String target = taskMap.get(taskID)[WORKER_URI];
			Workflow wf = Workflow.get(taskMap.get(taskID)[WORKFLOW_ID]);
			
			//Get a map for ready tasks in workflows for the target worker
			HashMap<Workflow, Set<String>> workflowReadyTasksMap = readyTaskMap.get(target);
			if(workflowReadyTasksMap == null)
			{
				workflowReadyTasksMap = new HashMap<>();
				readyTaskMap.put(target, workflowReadyTasksMap);
			}
				
			//Get a task list for the workflow
			Set<String> readyTaskSet = workflowReadyTasksMap.get(wf);
			if(readyTaskSet == null)
			{
				readyTaskSet = new HashSet<>();
				workflowReadyTasksMap.put(wf, readyTaskSet);
			}
			
			if(wf.isTaskReady(taskID, readyTaskSet))
			{
				readyTasks.add(taskID);
				readyTaskSet.add(taskID);
				taskMap.remove(taskID);
			}
		}
		
		
		//Generate sub-workflows for ready tasks
		HashMap<String, Set<Workflow>> readyWorkflow = new HashMap<>();
		for(Map.Entry<String, HashMap<Workflow, Set<String>>> entry: readyTaskMap.entrySet())
		{
			String worker = entry.getKey();
			HashMap<Workflow, Set<String>> subWorkflowMap = entry.getValue();
			for(Map.Entry<Workflow, Set<String>> workflowEntry : subWorkflowMap.entrySet())
			{
				Workflow oriWf = workflowEntry.getKey();
				if(workflowEntry.getValue().isEmpty())
				{
					continue;
				}
				Workflow subWf = oriWf.getSubworkflow(oriWf.getName(), workflowEntry.getValue());
				Set<Workflow> wfSet = readyWorkflow.get(worker);
				if(wfSet == null)
				{
					wfSet = new HashSet<>();
					readyWorkflow.put(worker, wfSet);
				}
				wfSet.add(subWf);
			}
		}
		
		//Remote tasks from queue
		taskQueue.removeAll(readyTasks);
		
		return readyWorkflow;
	}
	public String getTargetForTask(String taskUUID)
	{
		return taskMap.get(taskUUID)[WORKER_URI];
	}
	
	
	
	/**
	 * FOR MONITOR ONLY
	 */
	public String toHTML()
	{
		StringBuilder mappingHTML = new StringBuilder();
		for(Map.Entry<String, String> entry : this.getEntryQueue())
		{
			String tid = entry.getKey();
			String wkid = entry.getValue();
			Task t = Task.get(tid);
			mappingHTML.append("<div class=\"task-queue-entry\">[[")
					.append(t.getStatus().status)
					.append("]]")
					.append(t.getName())
					.append(":")
					.append(wkid)
					.append("</div>");
		}
		return mappingHTML.toString();
	}
	
	
	public Map.Entry<String, String>[] getEntryQueue()
	{
		Map.Entry<String, String>[] entries = new Map.Entry[taskQueue.size()];
		for(int i=0;i<taskQueue.size();i++)
		{
			String t = taskQueue.get(i);
			entries[i] = new QueueEntry(t, taskMap.get(t)[WORKER_URI]);
		}
		return entries;
	}
	private class QueueEntry implements Map.Entry<String, String>, Serializable
	{
		private String key;
		private String val;

		public QueueEntry(String key, String val)
		{
			this.key = key;
			this.val = val;
		}
		
		@Override
		public String getKey()
		{
			return key;
		}

		@Override
		public String getValue()
		{
			return val;
		}

		@Override
		public String setValue(String value)
		{
			val = value;
			return val;
		}
		
	}
	
}


