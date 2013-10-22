/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.server.filemanager.FileManager;
import workflowengine.utils.Threading;
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
	private TaskQueue taskQueue = new TaskQueue();
	private ExecutorNetwork execNetwork;
	private ExecutingProcessor[] processors;
	private String workingDir = "";
	private int workingProcessors = 0;
	
	protected Worker()   //throws RemoteException
	{
		super(true, "Worker@"+Utils.getProp("local_port"));
		totalProcessors = Runtime.getRuntime().availableProcessors();
		processors = new ExecutingProcessor[totalProcessors];
		execNetwork = new ExecutorNetwork();
		for(int i=0;i<totalProcessors;i++)
		{
			execNetwork.add(Integer.toString(i), Double.POSITIVE_INFINITY);
			processors[i] = new ExecutingProcessor(this);
		}
		
		workingDir = Utils.getProp("working_dir");
		manager.registerWorker(uri, totalProcessors);
	}
	
	public static WorkflowExecutor start() 
	{
		if(instant == null)
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
				synchronized(thisWorker)
				{
					logger.log("Workflow "+wf.getUUID()+" is submitted.");
					Utils.setProp(prop);
					wf.setSubmitted(Utils.time());
//					wf.save();
					wf.finalizedRemoteSubmit();

					for(String tid : wf.getTaskSet())
					{
						logger.log("Task "+Task.get(tid).getName()+" is submitted.");
					}

					for(String f : wf.getOutputFiles())
					{
						System.out.println(WorkflowFile.get(f).getName());
					}

					Utils.mkdirs(thisWorker.getWorkingDir()+"/"+wf.getSuperWfid());

					//Wait for all input file exists
					logger.log("Waiting for all input files...");
					for (String inputFileUUID : wf.getInputFiles())
					{
						WorkflowFile wff = WorkflowFile.get(inputFileUUID);
						if(wff == null)
						{
							System.out.println(inputFileUUID);
						}
						System.out.print("Waiting for "+wff.getName()+"...");
						FileManager.get().waitForFile(wff.getName(wf.getSuperWfid()));
						String fullFilePath = thisWorker.workingDir + "/" 
								+ wff.getName(wf.getSuperWfid());
						long size = new File(fullFilePath).length();
						wff.setSize(size);
						if(wff.getType() == WorkflowFile.TYPE_EXEC)
						{
							Utils.setExecutable(fullFilePath);
						}
						System.out.println("Done.");
					}
					logger.log("Done.", false);

					logger.log("Scheduling the submitted workflow...");
					Schedule s = thisWorker.getScheduler().getSchedule(
							new SchedulingSettings(thisWorker, wf, execNetwork, thisWorker.getDefaultFC()));
					s.save();
					logger.log("Done.", false);
					taskQueue.submit(s);
				}
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
		while(workingProcessors < totalProcessors && !taskQueue.isEmpty())
		{
			final ScheduleEntry se = taskQueue.poll();
			if(se!=null)
			{
				//logger.log("Starting execution of task "+se.taskUUID);
				Threading.submitTask(new Runnable(){
					@Override
					public void run()
					{
						processors[Integer.parseInt(se.target)].exec(Task.get(se.taskUUID), se);
					}
				});
				
				workingProcessors++;
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
		manager.setTaskStatus(status);
		
		if(status.status == TaskStatus.STATUS_COMPLETED)
		{
			//Upload output files
			Set<String> outFiles = new HashSet<>();
			for(String wff : Task.get(status.taskID).getOutputFiles())
			{
				outFiles.add(WorkflowFile.get(wff).getName(status.schEntry.superWfid));
			}
			FileManager.get().outputFilesCreated(outFiles);
			
			workingProcessors--;
			if(!taskQueue.isEmpty())
			{
				dispatchTask();
			}
		}
	}
	
	@Override
	public RemoteWorker getWorker(String uri)
	{
		return new RemoteWorker(uri, processors[Integer.parseInt(uri)]);
	}

	@Override
	public String getTaskQueueHTML()
	{
		return taskQueue.toHTML();
	}
	
	
	
	@Override
	public String getWorkingDir()
	{
		return workingDir;
	}
	
	@Override
	public Set<String> getWorkerSet()
	{
		if(execNetwork == null)
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
}
