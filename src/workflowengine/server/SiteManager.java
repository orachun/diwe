/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class SiteManager extends WorkflowExecutor
{
	private WorkflowExecutorInterface manager;
	private HashMap<String, RemoteWorker> remoteWorkers = new HashMap<>();
	protected static SiteManager instant;
	private int totalProcessors = 0;
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	
	protected SiteManager() throws RemoteException
	{
		super(true, "SiteManager@"+Utils.getProp("local_port"));
		if(
				Utils.hasProp("manager_host") 
				&& !Utils.getProp("manager_host").isEmpty() 
				&& Utils.hasProp("manager_port") 
				&& !Utils.getProp("manager_port").isEmpty()
				)
		{
			String managerURI = "//"+Utils.getProp("manager_host")+"/SiteManager@"+Utils.getProp("manager_port");
			while (manager == null)
			{
				try
				{
					manager = (WorkflowExecutorInterface) WorkflowExecutor.getRemoteExecutor(managerURI);
					manager.greeting("Hello from "+uri);
				}
				catch (NotBoundException ex)
				{
					manager = null;
				}
			}
		}
	}

	public static SiteManager start() 
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
	
	public void stop()
	{
		System.exit(0);
	}
	
	@Override
	public void submit(Workflow wf) 
	{
		wf.setSubmitted(Utils.time());
		wf.save();
		
		
		String workingDir = Utils.getProp("working_dir")+"/"+wf.getUUID();
		Utils.createDir(workingDir);
		
		//Wait for all input file exists
		for(String inputFileUUID : wf.getInputFiles())
		{
			WorkflowFile wff = WorkflowFile.get(inputFileUUID);
			String filename = workingDir+"/"+wff.getName();
			FileManager.get().waitForFile(filename);
		}
		
		Schedule s = this.getScheduler().getSchedule(new SchedulingSettings(this, wf, execNetwork, this.getDefaultFC()));
		FileManager.get().setSchedule(s);
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
			final WorkflowExecutorInterface we = remoteWorkers.get(entry.getKey()).getWorker();
			for (final Workflow wf : entry.getValue())
			{
				new Thread()
				{
					@Override
					public void run()
					{
						try
						{
							we.submit(wf);
						}
						catch (RemoteException ex)
						{
							logger.log("Cannot submit workflow to remote worker.", ex);
						}
					}
				}.start();
			}
		}
	}

	@Override
	public void setTaskStatus(TaskStatus status)  
	{
		Task.get(status.taskUUID).setStatus(status);
		
		if(manager != null)
		{
			try
			{
				manager.setTaskStatus(status);
			}
			catch (RemoteException ex)
			{
				logger.log("Cannot upload task status to manager.", ex);
			}
		}
		
	}

	@Override
	public void registerWorker(String uri, int totalProcessors)  
	{
		execNetwork.add(uri);
		this.totalProcessors += totalProcessors;
		RemoteWorker rw = null;
		while(rw == null)
		{
			try
			{
				rw = new RemoteWorker(uri);
			}
			catch (NotBoundException ex)
			{
				try
				{
					Thread.currentThread().wait(5000);
				}
				catch (InterruptedException ex1)
				{
					logger.log("Cannot connect to worker.", ex1);
				}
				rw = null;
			}
		}
		if(manager != null)
		{
			try
			{
				manager.registerWorker(this.getURI(), this.totalProcessors);
			}
			catch (RemoteException ex)
			{
				logger.log("Cannot update total processors to manager.", ex);
			}
		}
		remoteWorkers.put(uri, rw);
		try
		{
			rw.getWorker().greeting("Hello from "+this.getURI());
		}
		catch (RemoteException ex)
		{
			logger.log("Cannot send hello msg to worker.", ex);
		}
	}
	
	@Override
	public RemoteWorker getWorker(String uri)
	{
		return remoteWorkers.get(uri);
	}
	
	public static void main(String[] args)
	{
		SiteManager.start();
		
	}
}
