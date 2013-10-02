/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.rmi.RemoteException;
import java.util.Set;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.utils.SystemStats;
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
	protected static Worker instant;
	private TaskQueue taskQueue = new TaskQueue();
	private ExecutorNetwork execNetwork;
	private ExecutingProcessor[] processors;
	private String workingDir = "";
	private int workingProcessors = 0;
	
	protected Worker()  throws RemoteException
	{
		super(true, "Worker@"+Utils.getProp("local_port"));
		totalProcessors = Runtime.getRuntime().availableProcessors();
		processors = new ExecutingProcessor[totalProcessors];
		execNetwork = new ExecutorNetwork();
		for(char i=0;i<totalProcessors;i++)
		{
			execNetwork.add(String.valueOf(i), Double.POSITIVE_INFINITY);
			processors[i] = new ExecutingProcessor(this);
		}
		
		workingDir = Utils.getProp("working_dir");
		manager.registerWorker(uri, totalProcessors);
	}
	
	public static Worker start() 
	{
		if(instant == null)
		{
			try
			{
				instant = new Worker();
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
		final Worker thisWorker = this;
		new Thread("SUBMIT_WORKFLOW_TH")
		{
			@Override
			public void run()
			{
				logger.log("Workflow "+wf.getUUID()+" is submitted.");
				Utils.setProp(prop);
				wf.setSubmitted(Utils.time());
				wf.save();
				wf.finalizedRemoteSubmit();

				//Wait for all input file exists
				logger.log("Waiting for all input files...");
				for (String inputFileUUID : wf.getInputFiles())
				{
					WorkflowFile wff = WorkflowFile.get(inputFileUUID);
					FileManager.get().waitForFile(wff);
				}
				logger.log("Done.", false);

				logger.log("Scheduling the submitted workflow...");
				Schedule s = thisWorker.getScheduler().getSchedule(
						new SchedulingSettings(thisWorker, wf, execNetwork, thisWorker.getDefaultFC()));
				s.save();
				logger.log("Done.", false);
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
	public Set<String> getExecutorURIs()
	{
		return execNetwork.getExecutorURISet();
	}
	
	@Override
	public void dispatchTask()
	{
		while(workingProcessors < totalProcessors && !taskQueue.isEmpty())
		{
			final ScheduleEntry se = taskQueue.poll();
			if(se!=null)
			{
				logger.log("Starting execution of task "+se.taskUUID);
				new Thread(){

					@Override
					public void run()
					{
						processors[se.target.charAt(0)].exec(Task.get(se.taskUUID));
					}
				}.start();
				workingProcessors++;
			}
		}
	}

	@Override
	public void setTaskStatus(TaskStatus status)  
	{
		try
		{
			Task.get(status.taskID).setStatus(status);
			manager.setTaskStatus(status);
		}
		catch (RemoteException ex)
		{
			logger.log("Cannot upload task status to manager.", ex);
		}
		if(status.status == TaskStatus.STATUS_COMPLETED)
		{
			//Upload output files
			for(String wff : Task.get(status.taskID).getOutputFiles())
			{
				FileManager.get().outputCreated(WorkflowFile.get(wff));
			}
			
			workingProcessors--;
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
	public RemoteWorker getWorker(String uri)
	{
		return new RemoteWorker(uri, processors[uri.charAt(0)]);
	}

	@Override
	public String getTaskQueueHTML() throws RemoteException
	{
		return taskQueue.toHTML();
	}
	
	
	
	@Override
	public String getWorkingDir() throws RemoteException
	{
		return workingDir;
	}
	
	@Override
	public Set<String> getWorkerSet()
	{
		return null;
	}
	
	
	
	public static void main(String[] args)
	{
		Worker.start();
	}
}
