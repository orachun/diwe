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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleTable;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.Site;
import workflowengine.schedule.scheduler.Scheduler;
import static workflowengine.server.WorkflowExecutor.COST_PER_BYTE;
import static workflowengine.server.WorkflowExecutor.getRemoteExecutor;
import workflowengine.server.filemanager.DIFileManager;
import workflowengine.server.filemanager.FileManager;
import workflowengine.server.filemanager.FileServer;
import workflowengine.server.filemanager.ServerClientFileManager;
import workflowengine.server.filemanager.difm.NewDIFM;
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
	private final Map<String, Integer> remainingTasks = new ConcurrentHashMap<>();
	private ExecutorService setTaskStatusThreadPool = Executors.newFixedThreadPool(5);
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
					eventLogger.start("WF_SUBMIT", "");
					remainingTasks.put(wf.getUUID(), wf.getTotalTasks());
				}
				Utils.setProp(prop);
				wf.setSubmittedTime();
				wf.finalizedRemoteSubmit();
				wf.cache();
				Utils.mkdirs((thisSite.getWorkingDir() + "/" + wf.getSuperWfid()));
				if (wf.isDummy)
				{
					wf.createDummyInputFiles();
				}
				
				
				//Wait for all input file exists
				eventLogger.start("WF_INPUT_WAIT", "Waiting for all input files");
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
				eventLogger.finish("WF_INPUT_WAIT");

				eventLogger.start("SCHEDULING", "");
				logger.log("Scheduling the submitted workflow...");
				
//				Scheduler sch = Utils.getIntProp("dynamic") == 1? new HEFT() : thisSite.getScheduler();
				Scheduler sch = thisSite.getScheduler();
				
				Schedule s = sch.getSchedule(new SchedulingSettings(thisSite, wf, execNetwork, thisSite.getDefaultFC()));
				s.save();
				
				wf.setScheduledTime();
				logger.log("Done.", false);
				System.out.println("Estimated cost: " + s.getCost());
				System.out.println("Estimated makespan: " + s.getMakespan());
				System.out.println("Cum time: " +wf.getCumulatedExecTime());
				eventLogger.finish("SCHEDULING");

				logger.log("Submit scheduled tasks into the queue...");
				synchronized (DISPATCH_LOCK)
				{
					submitSchedule(s);
				}
				logger.log("Done.");
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
		else if(FileManager.get() instanceof NewDIFM)
		{
			taskQueue.submit(s);
			((NewDIFM)FileManager.get()).setSchedule(s);
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

	
	private int waititngDispatchThread = 0;
	@Override
	public void dispatchTask()
	{
		if(waititngDispatchThread > 1)
		{
			return;
		}
		waititngDispatchThread++;
		synchronized(DISPATCH_LOCK)
		{
			waititngDispatchThread--;
			Map<String, Set<Workflow>> se;
			do
			{
				se = taskQueue.pollNextReadyTasks();
				for (Map.Entry<String, Set<Workflow>> entry : se.entrySet())
				{
					String worker = entry.getKey();
					final WorkflowExecutorInterface we = remoteWorkers.get(worker).getWorker();
					for (final Workflow wf : entry.getValue())
					{
	//					logger.log("Dispatching subworkflow to " + worker + " ...");
						wf.prepareRemoteSubmit();
						we.submit(wf, null);
	//					logger.log("Done");
						for (String tid : wf.getTaskSet())
						{
							System.out.printf("Submit %s to %s\n", Task.get(tid).getName(), worker);
							TaskStatus status = TaskStatus.dispatchedStatus(tid);
							setTaskStatus(status);
						}
					}
				}
			}while(!se.isEmpty());
		}
	}

	
	
	@Override
	public void setTaskStatus(final TaskStatus status)
	{
		setTaskStatusThreadPool.submit(new Runnable()
		{
			@Override
			public void run()
			{try{

				final Task t = Task.get(status.taskID);
				if (status.status == TaskStatus.STATUS_EXECUTING)
				{
					//Inactivate file transferring if the file is not needed by any
					//incompleted tasks
					deactivateFilesIfNotUsed(t, status.schEntry.superWfid, true, false);
					
					runningTasks.put(status.taskID, status.schEntry.target);
				}
				else if (status.status == TaskStatus.STATUS_COMPLETED)
				{
					runningTasks.remove(status.taskID);

					

					//Inactivate file transferring if the file is not needed by any
					//incompleted tasks
					deactivateFilesIfNotUsed(t, status.schEntry.superWfid, true, true);
					
					updateRemainTasks(status);
					rescheduleIfNeeded(status);
				}
				else if(status.status == TaskStatus.STATUS_SUSPENDED)
				{
					runningTasks.remove(status.taskID);
				}

				if (status.status == TaskStatus.STATUS_COMPLETED)
				{
					dispatchTask();
				}}catch(Exception e){e.printStackTrace();}
			}
		});
		
		final Task t = Task.get(status.taskID);
//		synchronized (DISPATCH_LOCK)
//		{
			t.setStatus(status);
//		}
		logTaskStatus(status);
		
		if (manager != null && status.status == TaskStatus.STATUS_COMPLETED)
		{
			//Upload output files
			if (FileManager.get() instanceof ServerClientFileManager
					|| FileManager.get() instanceof NewDIFM )
			{
				Set<String> outFiles = new HashSet<>();
				for (String wff : t.getOutputFiles())
				{
					outFiles.add(WorkflowFile.get(wff).getName(status.schEntry.superWfid));
				}
				FileManager.get().outputFilesCreated(outFiles);
			}
		}
		
		if (manager != null)
		{
			status.schEntry.target = SiteManager.this.uri;
			manager.setTaskStatus(status);
		}
		
		System.out.println("Task "+t.getName()+" is "+status.status);
	}

	private void updateRemainTasks(TaskStatus status)
	{
		//Check if the workflow is completed or not
		Integer remainTasks;
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
			wf.setCompletedTime();
			double cost = getTotalCost();
			eventLogger.finish("WF_SUBMIT");
			System.out.println(wf.getName()+" is finished");
			System.out.println("Cost: "+cost);
			System.out.println("Makespan: "+(wf.getFinishedTime()-wf.getScheduledTime()));
			
			
			wf.saveStat(cost);
			
			
			if(Utils.hasProp("shutdown_when_workflow_finish") 
					&& Utils.getIntProp("shutdown_when_workflow_finish") == 1)
			{
				shutdown();
			}
		}
	}
	
	private void deactivateFilesIfNotUsed(final Task t, final String superWfid, final boolean input, final boolean output)
	{
		if(FileManager.get() instanceof ServerClientFileManager)
		{
			return;
		}
		Threading.submitTask(new Runnable()
		{
			@Override
			public void run()
			{
				Workflow wf = Workflow.get(superWfid);
				if (wf != null)
				{
//					Set<String> inactiveFiles = new HashSet<>();
					NewDIFM fileManager = (NewDIFM)FileManager.get();
					if(input)
					{
						for (String fid : t.getInputFiles())
						{
							if (!wf.isFileActive(fid))
							{
//								inactiveFiles.add(WorkflowFile.get(fid)
//										.getName(superWfid));
								fileManager.inactivateFile(WorkflowFile.get(fid)
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
//								inactiveFiles.add(WorkflowFile.get(fid)
//										.getName(superWfid));
								fileManager.inactivateFile(WorkflowFile.get(fid)
										.getName(superWfid));
							}
						}
					}
//					if (!inactiveFiles.isEmpty())
//					{
//						((DIFileManager) FileManager.get()).setInactiveFile(inactiveFiles);
//					}
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
				
				//Broadcast peer info
				for(String p : peers)
				{
					//Give all peer info to new added peer
					if(p.equals(uri))
					{
						((DIFileManager) FileManager.get()).setPeerSet(p, peers);
					}
					else //Give new peer info to the others
					{
						((DIFileManager) FileManager.get()).addPeer(p, uri);
					}
				}
			}
			else if (FileManager.get() instanceof NewDIFM)
			{
				((NewDIFM) FileManager.get()).workerConnected(uri);
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
		
//		for(String worker : getWorkerSet())
//		{
//			transferredBytes += getRemoteExecutor(worker).getTransferredBytes();
//		}
		return transferredBytes;
	}
	
	@Override
	public Set<SuspendedTaskInfo> suspendRunningTasks()
	{
		Set<SuspendedTaskInfo> allSuspendedTasks = new HashSet<>();
		for(String worker : getWorkerSet())
		{
			Set<SuspendedTaskInfo> suspendedTasks = getRemoteExecutor(worker).suspendRunningTasks();
			if(manager != null)
			{
				for (SuspendedTaskInfo s : suspendedTasks)
				{
					Task t = Task.get(s.tid);
					String superWfid =t.getSuperWfid();
					try
					{
						FileServer.request(
								this.getWorkingDir(),
								s.ckptFile.getName(superWfid),
								FileServer.TYPE_UPLOAD_REQ,
								Utils.getProp("manager_host"),
								FileServer.getPort(Utils.getIntProp("manager_port")));
					}
					catch (IOException ex)
					{
						logger.log("Cannot upload checkpointed data.", ex);
						return null;
					}
					setTaskStatus(t.getStatus().suspend());
				}
			}
			allSuspendedTasks.addAll(suspendedTasks);
		}
		
		return allSuspendedTasks;
	}
	
	
	private long lastRescheduling = -1;
	private boolean rescheduling = false;
	private static final double DELAY_TIME_THRESHOLD = 60;
	private static final double PERCENT_COST_REDUCE_THRESHOLD = 10;
	private static final double RESCHEDULING_INTERVAL = 100;
	public void rescheduleIfNeeded(TaskStatus status)
	{
		//Disable rescheduling feature
		if(true)
		{
			return;
		}
		
		//TODO: check when rescheduling is needed
		if(Utils.getIntProp("dynamic") == 0 || rescheduling)
		{
			return;
		}
		if(Utils.time() - lastRescheduling < RESCHEDULING_INTERVAL)
		{
			return;
		}
		
		Task task = Task.get(status.taskID);
		String superWfid = status.schEntry.superWfid;
		ScheduleTable oldSch = ScheduleTable.getSchduleForWorkflow(superWfid);
		
		if(Math.abs(oldSch.getEstStartTime(status.taskID) - task.getStartTime()) < DELAY_TIME_THRESHOLD)
		{
			return;
		}
		if(Math.abs(oldSch.getEstFinishTime(status.taskID) - task.getFinishedTime()) < DELAY_TIME_THRESHOLD)
		{
			return;
		}
		
		if(rescheduling)
		{
			return;
		}
		
		rescheduling = true;
		Workflow oriWf = Workflow.get(superWfid);
		double timeSinceStart = Utils.time() - oriWf.getScheduledTime();
		double costSinceStart = getTotalCost();
//		
//		synchronized(DISPATCH_LOCK)
//		{
			Workflow oldWf = Workflow.get(superWfid);
			Workflow wf = oldWf.getSubWorkflowOfRemainTasks();
			if(wf.getTotalTasks() == 0)
			{
				return;
			}
//			System.out.println("Rescheduling...");
			eventLogger.start("RESCH", "");

			Schedule s = getScheduler().getSchedule(
					new SchedulingSettings(this, wf, execNetwork, getDefaultFC()));
			
			HashMap<String, Site> siteMap = new HashMap<>();
			for(String worker : this.getWorkerSet())
			{
				siteMap.put(worker, new Site(worker, this.getWorker(worker).getTotalProcessors()));
			}
			
			double migrationCost = getMigrationCost();
			double oldCost = oldSch.getCurrentCost(costSinceStart, timeSinceStart, oldWf, siteMap, execNetwork);
			double newCost = s.getCost() + costSinceStart + migrationCost;
			double percentCostReduce = 100 * (oldCost - newCost)/oldCost;
			
			
//			System.out.println("New sch cost: "+s.getCost());
//			System.out.println("Migration cost: "+migrationCost);
			System.out.println("New cost: "+newCost);
			System.out.println("Old Current cost: "+oldCost);
			System.out.println("Old cost: "+oldSch.getCost());
			
			System.out.println("Percent Reduce: "+percentCostReduce);
			if (percentCostReduce > PERCENT_COST_REDUCE_THRESHOLD)
			{
				synchronized (DISPATCH_LOCK)
				{
					s.save();

					Set<SuspendedTaskInfo> suspendedTasks = suspendRunningTasks();
					System.out.println("Suspended: " + suspendedTasks.size());
					if (!suspendedTasks.isEmpty())
					{
						for (SuspendedTaskInfo sinfo : suspendedTasks)
						{
							sinfo.ckptFile.cache();
							Task t = Task.get(sinfo.tid);
							t.setCkptFid(sinfo.ckptFile.getUUID());
						}

						wf.generateInputOutputFileList();
						removeWorkflowFromQueue(superWfid);
		//				for(String tid : wf.getTaskSet())
		//				{
		//					Task t = Task.get(tid);
		//					if(t.getStatus().status != TaskStatus.STATUS_COMPLETED 
		//							&& t.getStatus().status != TaskStatus.STATUS_SUSPENDED)
		//					{
		//						t.setStatus(TaskStatus.waitingStatus(tid));
		//					}
		//				}
						taskQueue.submit(s);
					}
				}
			}
//		}
		lastRescheduling = Utils.time();
		rescheduling = false;
//		System.out.println("Done.");
		eventLogger.finish("RESCH");
	}
	
	private double getMigrationCost()
	{
		double migrateCost = 0;
		for(String runningTid : runningTasks.keySet())
		{
			double migrateFileSize = 0;
			Task runningTask = Task.get(runningTid);
			for(String wfid : runningTask.getInputFiles())
			{
				migrateFileSize += WorkflowFile.get(wfid).getSize();
			}
			for(String wfid : runningTask.getOutputFiles())
			{
				migrateFileSize += WorkflowFile.get(wfid).getSize();
			}
			migrateCost += COST_PER_BYTE*migrateFileSize;
			migrateCost += COST_PER_SECOND*5;
		}
		return migrateCost;
	}

	@Override
	public double getTotalCost()
	{
		double total = 0;
		for(String worker : getWorkerSet())
		{
			total += getRemoteExecutor(worker).getTotalCost();
		}
//		System.out.println("Workers' Cost: "+total);
//		System.out.println("Transfer cost: "+COST_PER_BYTE * getTransferredBytes());
		total = total + COST_PER_BYTE * getTransferredBytes();
		
		return total;
	}
	

	
	
}
