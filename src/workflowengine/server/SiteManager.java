/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
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
import workflowengine.server.filemanager.DIFileManager;
import workflowengine.server.filemanager.FileManager;
import workflowengine.server.filemanager.ServerClientFileManager;
import workflowengine.utils.Threading;
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
	private ExecutorNetwork execNetwork = new ExecutorNetwork();
	private TaskQueue taskQueue = new TaskQueue();
	private String workingDir;

	protected SiteManager() throws RemoteException
	{
		super(true, "SiteManager@" + Utils.getProp("local_port"));
		workingDir = Utils.getProp("working_dir");
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
				eventLogger.start("WF_RECEIVED:" + wf.getUUID(), "");
				Utils.setProp(prop);
				wf.setSubmitted(Utils.time());
//					wf.save();
				wf.finalizedRemoteSubmit();
				Cacher.cache(wf.getUUID(), wf);
				Utils.mkdirs((thisSite.workingDir + "/" + wf.getSuperWfid()));
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
					String fullFilePath = thisSite.workingDir + "/"
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
				synchronized (thisSite)
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
	public synchronized void dispatchTask()
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
					eventLogger.start("TASK_DISPATCH:" + Task.get(tid).getName() + tid, "");
				}
			}
		}
	}

	@Override
	public void setTaskStatus(final TaskStatus status)
	{
		final Task t = Task.get(status.taskID);
		synchronized (this)
		{
			t.setStatus(status);
		}

		if (status.status == TaskStatus.STATUS_EXECUTING)
		{
			eventLogger.finish("TASK_DISPATCH:" + Task.get(status.taskID).getName() + status.taskID);
			eventLogger.start("TASK_EXEC:" + Task.get(status.taskID).getName() + status.taskID, "");
		}
		else if (status.status == TaskStatus.STATUS_COMPLETED)
		{
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
				Threading.submitTask(new Runnable()
				{
					@Override
					public void run()
					{
						Workflow wf = Workflow.get(status.schEntry.superWfid);
						if (wf != null)
						{
							Set<String> inactiveFiles = new HashSet<>();
							for (String fid : t.getInputFiles())
							{
								if (!wf.isFileActive(fid))
								{
									inactiveFiles.add(WorkflowFile.get(fid)
											.getName(status.schEntry.superWfid));
								}
							}
							for (String fid : t.getOutputFiles())
							{
								if (!wf.isFileActive(fid) && !wf.getOutputFiles().contains(fid))
								{
									inactiveFiles.add(WorkflowFile.get(fid)
											.getName(status.schEntry.superWfid));
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
		}


		if (manager != null)
		{
			manager.setTaskStatus(status);
		}

		if (status.status == TaskStatus.STATUS_COMPLETED && !taskQueue.isEmpty())
		{
			dispatchTask();
		}
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
				try
				{
					rw = new RemoteWorker(uri, totalProcessors);
				}
				catch (Exception c)
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

			if (FileManager.get() instanceof DIFileManager)
			{
				((DIFileManager) FileManager.get()).workerJoined(uri);
				Set<String> peers = new HashSet<>(getWorkerSet());
				peers.add(uri);
				((DIFileManager) FileManager.get()).setPeerSet(uri, peers);
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
	public String getWorkingDir()
	{
		return workingDir;
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
}
