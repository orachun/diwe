/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Set;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.ExecutingProcessor;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.schedule.SchedulingSettings;
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
	protected WorkflowExecutorInterface manager;
	protected int totalProcessors;
	protected static Worker instant;
	private TaskQueue taskQueue = new TaskQueue();
	private ExecutorNetwork execNetwork;
	private ExecutingProcessor[] processors;
	private String workingDir;
	private int workingProcessors = 0;
	private static final Object INPUT_FILE_WAITING_LOCK = new Object();
	
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
		
		String managerURI = "//"+Utils.getProp("manager_host")+"/SiteManager@"+Utils.getProp("manager_port");
		while(manager == null)
		{
			try
			{
				manager = (WorkflowExecutorInterface)WorkflowExecutor.getRemoteExecutor(managerURI);
				manager.registerWorker(this.getURI(), totalProcessors);
				manager.greeting("Hello from "+uri);
			}
			catch (NotBoundException ex)
			{
				manager = null;
			}
		}
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
	public void submit(Workflow wf)  
	{
		wf.setSubmitted(Utils.time());
		wf.save();
		
		
		//Wait for all input file exists
		for(String inputFileUUID : wf.getInputFiles())
		{
			WorkflowFile wff = WorkflowFile.get(inputFileUUID);
			String filename = workingDir+"/"+wff.getName();
			FileManager.get().waitForFile(filename);
		}
		
		Schedule s = this.getScheduler().getSchedule(
				new SchedulingSettings(this, wf, execNetwork, this.getDefaultFC()));
		taskQueue.submit(s.getMapping());
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
		while(workingProcessors < totalProcessors && !taskQueue.isEmpty())
		{
			final ScheduleEntry se = taskQueue.poll();
			if(se!=null)
			{
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
			Task.get(status.taskUUID).setStatus(status);
			manager.setTaskStatus(status);
		}
		catch (RemoteException ex)
		{
			logger.log("Cannot upload task status to manager.", ex);
		}
		if(status.status == TaskStatus.STATUS_COMPLETED)
		{
			workingProcessors--;
			dispatchTask();
		}
	}
	
	@Override
	public RemoteWorker getWorker(String uri)
	{
		return new RemoteWorker(uri, processors[uri.charAt(0)]);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	public void printStat()
	{
		System.out.println("Available processors (cores): "
				+ Runtime.getRuntime().availableProcessors());

		/* Total amount of free memory available to the JVM */
		System.out.println("Free memory (bytes): "
				+ Runtime.getRuntime().freeMemory());

		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = Runtime.getRuntime().maxMemory();
		/* Maximum amount of memory the JVM will attempt to use */
		System.out.println("Maximum memory (bytes): "
				+ (maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory));

		/* Total memory currently in use by the JVM */
		System.out.println("Total memory (bytes): "
				+ Runtime.getRuntime().totalMemory());

		/* Get a list of all filesystem roots on this system */
		File[] roots = File.listRoots();

		/* For each filesystem root, print some info */
		for (File root : roots)
		{
			System.out.println("File system root: " + root.getAbsolutePath());
			System.out.println("Total space (bytes): " + root.getTotalSpace());
			System.out.println("Free space (bytes): " + root.getFreeSpace());
			System.out.println("Usable space (bytes): " + root.getUsableSpace());
		}
	}
	
	@Override
	public void registerWorker(String uri, int totalProcessors)  
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}

	public String getWorkingDir()
	{
		return workingDir;
	}
	
	
	public static void main(String[] args)
	{
		Worker.start();
	}
}
