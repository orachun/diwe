/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.schedule.SchedulingSettings;
import static workflowengine.server.WorkflowExecutor.getRemoteExecutor;
import workflowengine.server.filemanager.FileManager;
import workflowengine.server.filemanager.FileServer;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class Worker extends WorkflowExecutor
{
//	protected static Worker instant;
	private ExecutorNetwork execNetwork;
	private ExecutingProcessor[] processors;
	private int workingProcessors = 0;

	private ExecutorService execThreadPool;
	private ExecutorService setTaskStatusThreadPool;
	
	private Map<String, String> runningTasks = new ConcurrentHashMap<>(); // task -> processor
	
	protected Worker()   //throws RemoteException
	{
		super(true, "Worker@" + Utils.getProp("local_port"));
		totalProcessors = Runtime.getRuntime().availableProcessors();
		processors = new ExecutingProcessor[totalProcessors];
		execNetwork = new ExecutorNetwork();
		for (int i = 0; i < totalProcessors; i++)
		{
			execNetwork.add(Integer.toString(i), Double.POSITIVE_INFINITY);
			processors[i] = new ExecutingProcessor(this);
		}

		execThreadPool = Executors.newFixedThreadPool(totalProcessors);
		setTaskStatusThreadPool = Executors.newFixedThreadPool(totalProcessors);
		manager.registerWorker(uri, totalProcessors);
	}

	public static WorkflowExecutor start()
	{
		if (instant == null)
		{
			instant = new Worker();
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
		final Worker thisWorker = this;
		new Thread("SUBMIT_WORKFLOW_TH")
		{
			@Override
			public void run()
			{
				logger.log("Workflow " + wf.getUUID() + " is submitted.");
				Utils.setProp(prop);
				wf.setSubmitted(Utils.time());
				wf.finalizedRemoteSubmit();

				for (String tid : wf.getTaskSet())
				{
					logger.log("Task " + Task.get(tid).getName() + " is submitted.");
				}

				Utils.mkdirs(thisWorker.getWorkingDir() + "/" + wf.getSuperWfid());

				logger.log("Scheduling the submitted workflow...");
				Schedule s = thisWorker.getScheduler().getSchedule(
						new SchedulingSettings(thisWorker, wf, execNetwork, thisWorker.getDefaultFC()));
				s.save();
				logger.log("Done.", false);
				synchronized (thisWorker)
				{
					taskQueue.submit(s);
				}
				dispatchTask();
			}
		}.start();
	}

	private void waitForFile(String fid, String superWfid)
	{
		WorkflowFile wff = WorkflowFile.get(fid);
		System.out.print("Waiting for " + wff.getName() + "...");
		FileManager.get().waitForFile(wff.getName(superWfid));
		String fullFilePath = this.getWorkingDir() + "/"
				+ wff.getName(superWfid);
		long size = new File(fullFilePath).length();
		wff.setSize(size);
		if (wff.getType() == WorkflowFile.TYPE_EXEC)
		{
			Utils.setExecutable(fullFilePath);
		}
		System.out.println("Done.");
	}
	
	@Override
	public int getTotalProcessors()
	{
		return totalProcessors;
	}

	private int dispatchingThreads = 0;
	@Override
	public void dispatchTask()
	{
		if(dispatchingThreads > 1)
		{
			return;
		}
		synchronized(this)
		{
			dispatchingThreads++;
			while (workingProcessors < totalProcessors && !taskQueue.isEmpty())
			{
				final ScheduleEntry se = taskQueue.poll();
				if (se != null)
				{
					execThreadPool.submit(new Runnable()
					{
						@Override
						public void run()
						{
							Task t = Task.get(se.taskUUID);
							for(String fid : t.getInputFiles())
							{
								waitForFile(fid, se.superWfid);
							}
							logTaskStatus(TaskStatus.dispatchedStatus(t.getUUID()));
							processors[Integer.parseInt(se.target)].exec(Task.get(se.taskUUID), se);
						}
					});
					workingProcessors++;
				}
				else
				{
					System.out.println("No ready tasks");
					break;
				}
			}
			dispatchingThreads--;
		}
	}

	
	
	@Override
	public void setTaskStatus(final TaskStatus status)
	{
		setTaskStatusThreadPool.submit(new Runnable()
		{
			@Override
			public void run()
			{
				boolean taskCompleted = status.status == TaskStatus.STATUS_COMPLETED;
				Task.get(status.taskID).setStatus(status);
				
				
				if (taskCompleted)
				{
					//Upload output files
					Set<String> outFiles = new HashSet<>();
					for (String wff : Task.get(status.taskID).getOutputFiles())
					{
						outFiles.add(WorkflowFile.get(wff).getName(status.schEntry.superWfid));
					}
					FileManager.get().outputFilesCreated(outFiles);

					workingProcessors--;
					runningTasks.remove(status.taskID);
				}
				else if(status.status == TaskStatus.STATUS_EXECUTING)
				{
					runningTasks.put(status.taskID, status.schEntry.target);
				}
				else if(status.status == TaskStatus.STATUS_SUSPENDED)
				{
					workingProcessors--;
					runningTasks.remove(status.taskID);
				}
				
				status.schEntry.target = Worker.this.uri;
				
				if (taskCompleted && !taskQueue.isEmpty())
				{
					dispatchTask();
				}
			}
		});
		logTaskStatus(status);
		manager.setTaskStatus(status);
	}

	@Override
	public RemoteWorker getWorker(String uri)
	{
		return new RemoteWorker(uri, processors[Integer.parseInt(uri)]);
	}



	@Override
	public Set<String> getWorkerSet()
	{
		if (execNetwork == null)
		{
			return new HashSet<>();
		}
		return new HashSet<>(execNetwork.getExecutorURISet());
	}

	@Override
	public double getAvgBandwidth()
	{
		return -1;
	}

	public static void main(String[] args)
	{
		Utils.setPropFromArgs(args);
		Worker.start();
	}

	@Override
	public long getUsage()
	{
		long totalUsage = 0;
		for(ExecutingProcessor p : processors)
		{
			totalUsage += p.getUsage();
		}
		return totalUsage;
	}
	
	@Override
	public long getTransferredBytes()
	{
		long transferredBytes = FileManager.get().getTransferredBytes();
		return transferredBytes;
	}

	@Override
	public Set<SuspendedTaskInfo> suspendRunningTasks()
	{
		Set<SuspendedTaskInfo> allSuspendedTasks = new HashSet<>();
		for(ExecutingProcessor p : processors)
		{
			SuspendedTaskInfo sinfo = p.suspend();
			if(sinfo != null)
			{
				String superWfid = Task.get(sinfo.tid).getSuperWfid();
				try
				{
					logger.log("Uploading checkpoint data...");
					FileServer.request(
							this.getWorkingDir(), 
							sinfo.ckptFile.getName(superWfid), 
							FileServer.TYPE_UPLOAD_REQ, 
							Utils.getProp("manager_host"), 
							FileServer.getPort(Utils.getIntProp("manager_port")));
					logger.log("Done", false);
				}
				catch (IOException ex)
				{
					logger.log("Cannot upload checkpointed data.", ex);
					return null;
				}
				allSuspendedTasks.add(sinfo);
			}
		}
		return allSuspendedTasks;
	}
	
	@Override
	public void removeWorkflowFromQueue(String superWfid)
	{
		taskQueue.removeWorkflow(superWfid);
	}
	
}
