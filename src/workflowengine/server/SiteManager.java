/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import workflowengine.server.filemanager.FileManager;
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
import workflowengine.utils.db.Cacher;
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
//	protected static SiteManager instant;
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	
	protected SiteManager() throws RemoteException
	{
		super(true, "SiteManager@"+Utils.getProp("local_port"));
	}

	public static WorkflowExecutor start() 
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
				
				logger.log("Sub-workflow of "+wf.getSuperWfid()+" is submitted.");
				Utils.setProp(prop);
				wf.setSubmitted(Utils.time());
				wf.save();
				wf.finalizedRemoteSubmit();
				Cacher.cache(wf.getUUID(), wf);
				Utils.mkdirs((thisSite.getWorkingDir()+"/"+wf.getSuperWfid()));
				if(wf.isDummy)
				{
					wf.createDummyInputFiles();
				}

				//Wait for all input file exists
				logger.log("Waiting for all input files...");
				for(String inputFileUUID : wf.getInputFiles())
				{
					WorkflowFile wff = WorkflowFile.get(inputFileUUID);
					FileManager.get().waitForFile(wff.getName(wf.getSuperWfid()));
					long size = new File(Utils.getProp("working_dir") + "/" + wff.getName(wf.getSuperWfid())).length();
					wff.setSize(size);
				}
				logger.log("Done.", false);

				logger.log("Scheduling the submitted workflow...");
				Schedule s = thisSite.getScheduler().getSchedule(new SchedulingSettings(thisSite, wf, execNetwork, thisSite.getDefaultFC()));
				s.save();
				logger.log("Done.", false);

				logger.log("Submit scheduled tasks into the queue...");
				FileManager.get().setSchedule(s);
				taskQueue.submit(s);
				logger.log("Done.");
				
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
	public synchronized void dispatchTask()
	{
		Map<String, Set<Workflow>>  se = taskQueue.pollNextReadyTasks();
		for(Map.Entry<String, Set<Workflow>> entry : se.entrySet())
		{
			String workerURI = entry.getKey();
			final WorkflowExecutorInterface we = remoteWorkers.get(workerURI).getWorker();
			for (final Workflow wf : entry.getValue())
			{
				logger.log("Dispatching subworkflow to "+ workerURI);
				wf.prepareRemoteSubmit();
				we.submit(wf, null);
				for(String tid : wf.getTaskSet())
				{
					setTaskStatus(TaskStatus.dispatchedStatus(tid));
				}
			}
		}
	}

	@Override
	public void setTaskStatus(TaskStatus status)  
	{
		synchronized(this)
		{
			Task.get(status.taskID).setStatus(status);
		}
		if(manager != null)
		{
			manager.setTaskStatus(status);
		}
		
		
		if(status.status == TaskStatus.STATUS_COMPLETED)
		{
			//Upload output files
//			if(manager!=null)
//			{
//				for(String wff : Task.get(status.taskID).getOutputFiles())
//				{
//					FileManager.get().outputCreated(WorkflowFile.get(wff), status.schEntry.wfDir);
//				}
//			}
			
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
		RemoteWorker rw = null;
		if(remoteWorkers.containsKey(uri))
		{
			rw = remoteWorkers.get(uri);
			this.totalProcessors = this.totalProcessors 
					- rw.getTotalProcessors() 
					+ totalProcessors;
			rw.setTotalProcessors(totalProcessors);
		}
		else
		{
			execNetwork.add(uri);
			this.totalProcessors += totalProcessors;
			while(rw == null)
			{
				try
				{
					rw = new RemoteWorker(uri, totalProcessors);
				}
				catch(Exception c)
				{
					try
					{
						Thread.sleep(5000);
					}
					catch (InterruptedException ex1)
					{
						logger.log("Cannot connect to worker.", ex1);
					}
					rw = null;
				}
			}
			remoteWorkers.put(uri, rw);
			avgBandwidth = -1;
			FileManager.get().broadcaseWorkerJoined(uri);
			Set<String> peers = new HashSet<>(getWorkerSet());
			peers.add(uri);
//			FileManager.get().broadcastPeerSet(peers);
			FileManager.getRemoteFileManager(uri).setPeerSet(peers);
		}
		if(manager != null)
		{
			manager.registerWorker(this.getURI(), this.totalProcessors);
		}
		rw.getWorker().greeting("Hello from "+this.getURI());
	}

	@Override
	public double getAvgBandwidth()
	{
		if(avgBandwidth != -1)
		{
			return avgBandwidth;
		}
		
		Set<String> workers = getWorkerSet();
		double sum = 0;
		int count = 0;
		for(String w : workers)
		{
			WorkflowExecutorInterface worker;
			worker = WorkflowExecutor.getRemoteExecutor(w);
			double workerBW = worker.getAvgBandwidth();
			if(workerBW != -1)
			{
				int workerProcs = worker.getTotalProcessors();
				sum += workerBW*workerProcs;
				count += workerProcs;
			}
			sum += execNetwork.getLinkSpd(w);
			count ++;
		}
		
		avgBandwidth = sum / count;
		if(avgBandwidth < 1)
		{
			avgBandwidth = 1;
		}
		return avgBandwidth;
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
		if(execNetwork == null)
		{
			return new HashSet<>();
		}
		return execNetwork.getExecutorURISet();
	}
	
	
	
	
	
	
	
	
	
	
	public static void main(String[] args)
	{
		Utils.setPropFromArgs(args);
		SiteManager.start();
	}
}
