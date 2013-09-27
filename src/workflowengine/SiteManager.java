/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulerSettings;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class SiteManager extends WorkflowExecutor
{
	private SiteManager manager;
	private HashMap<String, WorkflowExecutor> workerMap = new HashMap<>();
	protected static SiteManager instant;
	private int totalProcessors = 0;
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	
	protected SiteManager() throws RemoteException
	{
		//TODO: get managerURI if esixt
		String managerURI = "";
		while(manager == null)
		{
			try
			{
				manager = (SiteManager)WorkflowExecutor.getRemoteExecutor(managerURI);
			}
			catch (NotBoundException ex)
			{
				manager = null;
			}
		}
	}

	public static SiteManager get() 
	{
		if(instant == null)
		{
			try
			{
				instant = new SiteManager();
			}
			catch (RemoteException ex)
			{
				throw new RuntimeException(ex);
			}
		}
		return instant;
	}
	
	@Override
	public void submit(Workflow wf) 
	{
		//TODO: wait until all input files exist
		
		Schedule s = this.getScheduler().getSchedule(new SchedulerSettings(wf, execNetwork, this.getDefaultFC()));
		taskQueue.submit(s);
		dispatchTask();
	}

	@Override
	public int getTotalProcessors() 
	{
		return totalProcessors;
	}

	@Override
	public Set<String> getExecutorURIs()
	{
		return execNetwork.getExecutorURISet();
	}

	@Override
	public void dispatchTask()
	{
		Map<String, Set<Workflow>>  se = taskQueue.pollNextReadyTasks();
		for(Map.Entry<String, Set<Workflow>> entry : se.entrySet())
		{
			WorkflowExecutor we = workerMap.get(entry.getKey());
			for(Workflow wf : entry.getValue())
			{
				we.submit(wf);
			}
		}
	}

	@Override
	public void setTaskStatus(TaskStatus status)  
	{
		//TODO: store task status to local db
		manager.setTaskStatus(status);
		
		throw new UnsupportedOperationException("Not supported.");
	}

	@Override
	public void registerWorker(String uri, int totalProcessors)  
	{
		execNetwork.add(uri);
		this.totalProcessors += totalProcessors;
		WorkflowExecutor worker = null;
		while(worker == null)
		{
			try
			{
				worker = WorkflowExecutor.getRemoteExecutor(uri);
			}
			catch (NotBoundException ex)
			{
				worker = null;
				try
				{
					Thread.currentThread().wait(5000);
				}
				catch (InterruptedException ex1){}
			}
		}
		workerMap.put(uri, worker);
	}
	
	
}
