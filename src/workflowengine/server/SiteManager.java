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
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class SiteManager extends WorkflowExecutor
{
	private SiteManager manager;
	private HashMap<String, RemoteWorker> remoteWorkers = new HashMap<>();
	protected static SiteManager instant;
	private int totalProcessors = 0;
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	private static final Object INPUT_FILE_WAITING_LOCK = new Object();
	
	protected SiteManager() throws RemoteException
	{
		super(true, "SiteManager@"+Utils.getProp("local_port"));
		if(Utils.hasProp("manager_host") && !Utils.getProp("manager_host").isEmpty() && Utils.hasProp("manager_port") && !Utils.getProp("manager_port").isEmpty())
		{
			String managerURI = "//"+Utils.getProp("manager_host")+"/SiteManager@"+Utils.getProp("manager_port");
			while (manager == null)
			{
				try
				{
					manager = (SiteManager) WorkflowExecutor.getRemoteExecutor(managerURI);
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
			while(!Utils.fileExists(workingDir+"/"+wff.getName()))
			{
				synchronized(INPUT_FILE_WAITING_LOCK)
				{
					try
					{
						INPUT_FILE_WAITING_LOCK.wait();
					}
					catch (InterruptedException ex){}
				}
			}
		}
		
		Schedule s = this.getScheduler().getSchedule(new SchedulingSettings(this, wf, execNetwork, this.getDefaultFC()));
		taskQueue.submit(s);
		dispatchTask();
	}

	public void notifyInputFileArrival()
	{
		synchronized(INPUT_FILE_WAITING_LOCK)
		{
			INPUT_FILE_WAITING_LOCK.notifyAll();
		}
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
			WorkflowExecutorInterface we = remoteWorkers.get(entry.getKey()).getWorker();
			for(Workflow wf : entry.getValue())
			{
				try
				{
					we.submit(wf);
				}
				catch (RemoteException ex)
				{
					Logger.getLogger(SiteManager.class.getName()).log(Level.SEVERE, null, ex);
				}
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
					Logger.getLogger(SiteManager.class.getName()).log(Level.SEVERE, null, ex1);
				}
				rw = null;
			}
		}
		if(manager != null)
		{
			manager.registerWorker(this.getURI(), this.totalProcessors);
		}
		remoteWorkers.put(uri, rw);
		try
		{
			rw.getWorker().greeting("Hello from "+this.getURI());
		}
		catch (RemoteException ex)
		{
			Logger.getLogger(SiteManager.class.getName()).log(Level.SEVERE, null, ex);
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
