/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
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
	private HashMap<String, RemoteWorker> remoteWorkers = new HashMap<>();
	protected static SiteManager instant;
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	
	protected SiteManager() throws RemoteException
	{
		super(true, "SiteManager@"+Utils.getProp("local_port"));
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
	public void submit(final Workflow wf, final java.util.Properties prop) 
	{
		final SiteManager thisSite = this;
		new Thread("WORKFLOW_SUBMIT_TH")
		{
			@Override
			public void run()
			{
				logger.log("Workflow "+wf.getUUID()+" is submitted.");
				Utils.setProp(prop);
				wf.setSubmitted(Utils.time());
				wf.save();
				wf.finalizedRemoteSubmit();
				Utils.mkdirs((thisSite.getWorkingDir()+"/"+wf.getSuperWfid()));
				if(wf.isDummy)
				{
					wf.createDummyInputFiles(thisSite.getWorkingDir()+"/"+wf.getSuperWfid());
				}

				//Wait for all input file exists
				logger.log("Waiting for all input files...");
				for(String inputFileUUID : wf.getInputFiles())
				{
					WorkflowFile wff = WorkflowFile.get(inputFileUUID);
					FileManager.get().waitForFile(wff, wf.getSuperWfid());
				}
				logger.log("Done.", false);

				logger.log("Scheduling the submitted workflow...");
				Schedule s = thisSite.getScheduler().getSchedule(new SchedulingSettings(thisSite, wf, execNetwork, thisSite.getDefaultFC()));
				s.save();
				logger.log("Done.", false);

				FileManager.get().setSchedule(s);
				taskQueue.submit(s);
				dispatchTask();
			}
		}.start();
	}

	@Override
	public int getTotalProcessors() 
	{
		return totalProcessors;
	}


	@Override
	public void dispatchTask()
	{
		Map<String, Set<Workflow>>  se = taskQueue.pollNextReadyTasks();
		for(Map.Entry<String, Set<Workflow>> entry : se.entrySet())
		{
			String workerURI = entry.getKey();
			final WorkflowExecutorInterface we = remoteWorkers.get(workerURI).getWorker();
			for (final Workflow wf : entry.getValue())
			{
				logger.log("Dispatching subworkflow "+wf.getUUID()+ " to "+ workerURI);
				wf.prepareRemoteSubmit();
					we.submit(wf, null);
//				try
//				{
//					we.submit(wf, null);
//				}
//				catch (RemoteException ex)
//				{
//					logger.log("Cannot submit workflow to remote worker.", ex);
//				}
			}
		}
	}

	@Override
	public void setTaskStatus(TaskStatus status)  
	{
		Task.get(status.taskID).setStatus(status);
		
		if(manager != null)
		{
				manager.setTaskStatus(status);
//			try
//			{
//				manager.setTaskStatus(status);
//			}
//			catch (RemoteException ex)
//			{
//				logger.log("Cannot upload task status to manager.", ex);
//			}
		}
		
		
		if(status.status == TaskStatus.STATUS_COMPLETED)
		{
			//Upload output files
			if(manager!=null)
			{
				for(String wff : Task.get(status.taskID).getOutputFiles())
				{
					FileManager.get().outputCreated(WorkflowFile.get(wff), status.schEntry.wfDir);
				}
			}
			
			
			
			if(taskQueue.isEmpty())
			{
				logger.log("Task queue is empty.");
			}
			else
			{
				dispatchTask();
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
				rw = new RemoteWorker(uri, totalProcessors);
			}
			catch (NotBoundException ex)
			{
				try
				{
					Thread.currentThread().sleep(5000);
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
				manager.registerWorker(this.getURI(), this.totalProcessors);
//			try
//			{
//			}
//			catch (RemoteException ex)
//			{
//				logger.log("Cannot update total processors to manager.", ex);
//			}
		}
		remoteWorkers.put(uri, rw);
			rw.getWorker().greeting("Hello from "+this.getURI());
//		try
//		{
//		}
//		catch (RemoteException ex)
//		{
//			logger.log("Cannot send hello msg to worker.", ex);
//		}
	}
	
	@Override
	public RemoteWorker getWorker(String uri)
	{
		return remoteWorkers.get(uri);
	}
	
	@Override
	public String getWorkingDir()
	{
		return Utils.getProp("working_dir");
	}

	
	
	
	
	
	
	@Override
	public String getTaskQueueHTML()
	{
		return taskQueue.toHTML();
	}

	@Override
	public Set<String> getWorkerSet()
	{
		return new HashSet<>(execNetwork.getExecutorURISet());
	}
	
	
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args)
	{
		Utils.setPropFromArgs(args);
		SiteManager.start();
	}
}
