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
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.utils.db.Cacher;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class TaskQueue implements Serializable
{ 
	//TaskUUID  -> (WorkflowID, Worker URI)
	private static final int WORKFLOW_ID = 0;
	private static final int WORKER_URI = 1;
	
	protected Map<String, String[]> taskMap = new HashMap<>(); 
	protected LinkedList<String> taskQueue = new LinkedList<>(); //Task UUID
		
	public void submit(Schedule s)
	{
		taskQueue.addAll(s.getMapping().keySet());
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
	public ScheduleEntry poll()
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
	 * Poll ready tasks as sub-workflows for each target
	 * @return a map between a target string and a set of sub-workflows
	 */
	public Map<String, Set<Workflow>> pollNextReadyTasks()
	{
		//Site -> (Workflow, Task Set)		
		HashMap<String, HashMap<Workflow, List<String>>> readyTaskMap = new HashMap<>();
		ListIterator<String> iterator = taskQueue.listIterator();
		
		String taskID;
		Set<String> readyTasks = new HashSet<>();
		while(iterator.hasNext())
		{
			taskID = iterator.next();
			Workflow wf = (Workflow)Cacher.get(Workflow.class, taskMap.get(taskID)[WORKFLOW_ID]);
			if(wf.isTaskReady(taskID))
			{
				readyTasks.add(taskID);
				String target = taskMap.get(taskID)[WORKER_URI];
				
				//Get a map for ready sub-workflow for the target worker
				HashMap<Workflow, List<String>> workflowReadyTasksMap = readyTaskMap.get(target);
				if(workflowReadyTasksMap == null)
				{
					workflowReadyTasksMap = new HashMap<>();
					readyTaskMap.put(target, workflowReadyTasksMap);
				}
				
				
				//Get a task list for the workflow
				List<String> readyTaskList = workflowReadyTasksMap.get(wf);
				if(readyTaskList == null)
				{
					readyTaskList = new LinkedList<>();
					workflowReadyTasksMap.put(wf, readyTaskList);
				}
				readyTaskList.add(taskID);
				
				taskMap.remove(taskID);
			}
		}
		
		//Generate sub-workflows for ready tasks
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
			mappingHTML.append("<div>[")
					.append(t.getStatus().status)
					.append("]")
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


