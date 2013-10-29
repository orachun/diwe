/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.server.filemanager.DIFileManager;
import workflowengine.server.filemanager.FileManager;
import workflowengine.server.filemanager.FileServer;
import workflowengine.server.filemanager.ServerClientFileManager;
import workflowengine.utils.Threading;
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
	private final Object DISPATCH_LOCK = new Object();
	private HashMap<String, RemoteWorker> remoteWorkers = new HashMap<>();
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	private final Map<String, Integer> remainingTasks = new ConcurrentHashMap<>();
	private Map<String, String> runningTasks = new ConcurrentHashMap<>(); // task -> processor

	protected SiteManager() throws RemoteException
	{
		super(true, "SiteManager@" + Utils.getProp("local_port"));
	}

	public static WorkflowExecutor start()
	{
		if (instant == null)
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

				logger.log("Sub-workflow of " + wf.getSuperWfid() + " is submitted.");
				
				if(!wf.isSubworkflow())
				{
					eventLogger.start("WF_SUBMIT:" + wf.getUUID(), "");
					remainingTasks.put(wf.getUUID(), wf.getTotalTasks());
				}
				Utils.setProp(prop);
				wf.setSubmitted(Utils.time());
//					wf.save();
				wf.finalizedRemoteSubmit();
				wf.cache();
				Utils.mkdirs((thisSite.getWorkingDir() + "/" + wf.getSuperWfid()));
				if (wf.isDummy)
				{
					wf.createDummyInputFiles();
				}
				
				
				//Wait for all input file exists
				eventLogger.start("WF_INPUT_WAIT:" + wf.getUUID(), "Waiting for all input files");
				logger.log("Waiting for all input files...");
				for (String inputFileUUID : wf.getInputFiles())
				{
					WorkflowFile wff = WorkflowFile.get(inputFileUUID);
					FileManager.get().waitForFile(wff.getName(wf.getSuperWfid()));
					String fullFilePath = thisSite.getWorkingDir() + "/"
							+ wff.getName(wf.getSuperWfid());
					long size = new File(fullFilePath).length();
					wff.setSize(size);
					if (wff.getType() == WorkflowFile.TYPE_EXEC)
					{
						Utils.setExecutable(fullFilePath);
					}
				}
				logger.log("Done.", false);
				eventLogger.finish("WF_INPUT_WAIT:" + wf.getUUID());

				eventLogger.start("SCHEDULING:" + wf.getUUID(), "");
				logger.log("Scheduling the submitted workflow...");
				Schedule s = thisSite.getScheduler().getSchedule(new SchedulingSettings(thisSite, wf, execNetwork, thisSite.getDefaultFC()));
				s.save();
				logger.log("Done.", false);
				logger.log("Estimated makespan: " + s.getMakespan());
				eventLogger.finish("SCHEDULING:" + wf.getUUID());

				logger.log("Submit scheduled tasks into the queue...");
				synchronized (DISPATCH_LOCK)
				{
					submitSchedule(s);
					logger.log("Done.");
				}
				dispatchTask();
			}
		}.start();
	}

	private void submitSchedule(final Schedule s)
	{
		if (FileManager.get() instanceof DIFileManager)
		{
			Thread t = new Thread()
			{
				@Override
				public void run()
				{
					((DIFileManager) FileManager.get()).setSchedule(s);
				}
			};
			t.start();
			taskQueue.submit(s);
			try
			{
				t.join();
			}
			catch (InterruptedException ex)
			{
				logger.log("Error: " + ex.getMessage(), ex);
			}
		}
		else
		{
			taskQueue.submit(s);
		}
	}

	@Override
	public int getTotalProcessors()
	{
		return totalProcessors;
	}

	@Override
	public void dispatchTask()
	{
		synchronized(DISPATCH_LOCK)
		{
			Map<String, Set<Workflow>> se = taskQueue.pollNextReadyTasks();
			for (Map.Entry<String, Set<Workflow>> entry : se.entrySet())
			{
				String workerURI = entry.getKey();
				final WorkflowExecutorInterface we = remoteWorkers.get(workerURI).getWorker();
				for (final Workflow wf : entry.getValue())
				{
					logger.log("Dispatching subworkflow to " + workerURI + " ...");
					wf.prepareRemoteSubmit();
					we.submit(wf, null);
					logger.log("Done");
					for (String tid : wf.getTaskSet())
					{
						setTaskStatus(TaskStatus.dispatchedStatus(tid));
						Task t = Task.get(tid);
						if(t.getStatus().status == TaskStatus.STATUS_WAITING)
						{
							eventLogger.start("TASK_DISPATCH:" + t.getName() + tid, "");
						}
					}
				}
			}
		}
	}

	@Override
	public void setTaskStatus(final TaskStatus status)
	{
		new Thread()
		{
			@Override
			public void run()
			{
				final Task t = Task.get(status.taskID);
				synchronized (DISPATCH_LOCK)
				{
					t.setStatus(status);
				}

				if (status.status == TaskStatus.STATUS_EXECUTING)
				{
					String dispatchEventKey = "TASK_DISPATCH:" 
							+ Task.get(status.taskID).getName() + status.taskID;
					if(eventLogger.eventFinished(dispatchEventKey))
					{
						eventLogger.finish(dispatchEventKey);
					}
					
					String execEventKey = "TASK_EXEC:" 
							+ Task.get(status.taskID).getName() + status.taskID;
					if(!eventLogger.hasEvent(execEventKey))
					{
						eventLogger.start(execEventKey, "");
					}

					//Inactivate file transferring if the file is not needed by any
					//incompleted tasks
					if (FileManager.get() instanceof DIFileManager)
					{
						deactivateFilesIfNotUsed(t, status.schEntry.superWfid, true, false);
					}
					runningTasks.put(status.taskID, status.schEntry.target);
				}
				else if (status.status == TaskStatus.STATUS_COMPLETED)
				{
					runningTasks.remove(status.taskID);
					eventLogger.finish("TASK_EXEC:" + Task.get(status.taskID).getName() + status.taskID);

					//Upload output files
					if (manager != null && FileManager.get() instanceof ServerClientFileManager)
					{
						Set<String> outFiles = new HashSet<>();
						for (String wff : Task.get(status.taskID).getOutputFiles())
						{
							outFiles.add(WorkflowFile.get(wff).getName(status.schEntry.superWfid));
						}
						FileManager.get().outputFilesCreated(outFiles);
					}

					//Inactivate file transferring if the file is not needed by any
					//incompleted tasks
					if (FileManager.get() instanceof DIFileManager)
					{
						deactivateFilesIfNotUsed(t, status.schEntry.superWfid, true, true);
					}

					updateRemainTasks(status);
					
					rescheduleIfNeeded(status.schEntry.superWfid);
				}


				if (manager != null)
				{
					status.schEntry.target = SiteManager.this.uri;
					manager.setTaskStatus(status);
				}

				if (status.status == TaskStatus.STATUS_COMPLETED && !taskQueue.isEmpty())
				{
					dispatchTask();
				}
			}
		}.start();
	}

	private void updateRemainTasks(TaskStatus status)
	{
		//Check if the workflow is completed or not
		Integer remainTasks = null;
		synchronized(remainingTasks)
		{
			remainTasks = remainingTasks.get(status.schEntry.superWfid);
			if(remainTasks != null)
			{
				remainTasks--;
				remainingTasks.put(status.schEntry.superWfid, remainTasks);
			}
		}
		if(remainTasks != null && remainTasks.equals(0))
		{
			Workflow wf = Workflow.get(status.schEntry.superWfid);
			wf.setFinishedTime(Utils.time());
			eventLogger.finish("WF_SUBMIT:" + status.schEntry.superWfid);
			System.out.println(wf.getName()+" is finished");
		}
	}
	
	private void deactivateFilesIfNotUsed(final Task t, final String superWfid, final boolean input, final boolean output)
	{
		Threading.submitTask(new Runnable()
		{
			@Override
			public void run()
			{
				Workflow wf = Workflow.get(superWfid);
				if (wf != null)
				{
					Set<String> inactiveFiles = new HashSet<>();
					if(input)
					{
						for (String fid : t.getInputFiles())
						{
							if (!wf.isFileActive(fid))
							{
								inactiveFiles.add(WorkflowFile.get(fid)
										.getName(superWfid));
							}
						}
					}
					if(output)
					{
						for (String fid : t.getOutputFiles())
						{
							if (!wf.isFileActive(fid) && !wf.getOutputFiles().contains(fid))
							{
								inactiveFiles.add(WorkflowFile.get(fid)
										.getName(superWfid));
							}
						}
					}
					if (!inactiveFiles.isEmpty())
					{
						((DIFileManager) FileManager.get()).setInactiveFile(inactiveFiles);
					}
				}
			}
		});
	}
	
	@Override
	public void registerWorker(String uri, int totalProcessors)
	{
		RemoteWorker rw = null;
		if (remoteWorkers.containsKey(uri))
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
			while (rw == null)
			{
				rw = new RemoteWorker(uri, totalProcessors);
			}
			remoteWorkers.put(uri, rw);
			avgBandwidth = -1;

			if (FileManager.get() instanceof DIFileManager)
			{
				((DIFileManager) FileManager.get()).workerJoined(uri);
				Set<String> peers = new HashSet<>(getWorkerSet());
				peers.add(this.uri);
				for(String p : peers)
				{
					if(p.equals(uri))
					{
						((DIFileManager) FileManager.get()).setPeerSet(p, peers);
					}
					else
					{
						((DIFileManager) FileManager.get()).addPeer(p, uri);
					}
				}
			}
		}
		if (manager != null)
		{
			manager.registerWorker(this.getURI(), this.totalProcessors);
		}
		rw.getWorker().greeting("Hello from " + this.getURI());
	}

	@Override
	public double getAvgBandwidth()
	{
		if (avgBandwidth != -1)
		{
			return avgBandwidth;
		}

		Set<String> workers = getWorkerSet();
		double sum = 0;
		int count = 0;
		for (String w : workers)
		{
			WorkflowExecutorInterface worker;
			worker = WorkflowExecutor.getRemoteExecutor(w);
			double workerBW = worker.getAvgBandwidth();
			
			//If worker is a site manager
			if (workerBW != -1)
			{
				int workerProcs = worker.getTotalProcessors();
				sum += workerBW * workerProcs;
				count += workerProcs;
			}
			
			sum += execNetwork.getLinkSpd(w);
			count++;
		}

		avgBandwidth = sum / count;
		if (avgBandwidth < 1)
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
	public String getTaskQueueHTML()
	{
		return taskQueue.toHTML();
	}

	@Override
	public Set<String> getWorkerSet()
	{
		if (execNetwork == null)
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
	
	
	@Override
	public long getUsage()
	{
		long totalUsage = 0;
		for(String worker : getWorkerSet())
		{
			totalUsage += getRemoteExecutor(worker).getUsage();
		}
		return totalUsage;
	}
	
	@Override
	public long getTransferredBytes()
	{
		long transferredBytes = FileManager.get().getTransferredBytes();
		
		for(String worker : getWorkerSet())
		{
			transferredBytes += getRemoteExecutor(worker).getTransferredBytes();
		}
		return transferredBytes;
	}
	
	@Override
	public WorkflowFile suspend(String tid)
	{
		String worker = runningTasks.get(tid);
		if(worker == null)
		{
			return null;
		}
		WorkflowFile ckptFile = getRemoteExecutor(worker).suspend(tid);
		String superWfid = Task.get(tid).getSuperWfid();
		if(manager != null)
		{
			try
			{
				FileServer.request(
						this.getWorkingDir(), 
						ckptFile.getName(superWfid), 
						FileServer.UPLOAD_REQ_TYPE, 
						Utils.getProp("manager_host"), 
						FileServer.DEFAULT_PORT);
			}
			catch (IOException ex)

			{
				logger.log("Cannot upload checkpointed data.", ex);
				return null;
			}
		}
		return ckptFile;
	}
	private long lastRescheduling = -1;
	private boolean rescheduling = false;
	public void rescheduleIfNeeded(String superWfid)
	{
		//TODO: check when rescheduling is needed
		if(!rescheduling && Utils.time() - lastRescheduling > 10)
		{
			rescheduling = true;
			System.out.println("Rescheduling...");
			String eventKey = eventLogger.start("RESCH"+Utils.timeMillis(), "");
			synchronized(DISPATCH_LOCK)
			{
				Workflow wf = Workflow.get(superWfid).getSubWorkflowOfRemainTasks();
				if(wf.getTotalTasks() == 0)
				{
					return;
				}

				Schedule s = getScheduler().getSchedule(
						new SchedulingSettings(this, wf, execNetwork, getDefaultFC()));
				s.save();
				taskQueue.update(s);

				for(Map.Entry<String, String> entry : runningTasks.entrySet())
				{
					String tid = entry.getKey();
					String worker = entry.getValue();
					WorkflowFile ckptFile = getRemoteExecutor(worker).suspend(tid);
					if(ckptFile != null)
					{
						ckptFile.cache();
						Task t = Task.get(tid);
						t.setCkptFid(ckptFile.getUUID());
						
					}
				}
			}
			lastRescheduling = Utils.time();
			System.out.println("Done.");
			eventLogger.finish(eventKey);
			rescheduling = false;
		}
	}
}
