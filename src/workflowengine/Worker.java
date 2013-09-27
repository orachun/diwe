/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Set;
import workflowengine.workflow.TaskStatus;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.resource.Processor;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.schedule.SchedulerSettings;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public class Worker extends WorkflowExecutor
{
	protected SiteManager manager;
	protected int totalProcessors;
	protected static Worker instant;
	private TaskQueue taskQueue = new TaskQueue();
	private ExecutorNetwork execNetwork;
	private Processor[] processors;
	private String workingDir;
	private int workingProcessors = 0;

	
	
	protected Worker()  throws RemoteException
	{
		totalProcessors = Runtime.getRuntime().availableProcessors();
		processors = new Processor[totalProcessors];
		execNetwork = new ExecutorNetwork();
		for(char i=0;i<totalProcessors;i++)
		{
			execNetwork.add(String.valueOf(i), Double.POSITIVE_INFINITY);
			processors[i] = new Processor(this);
		}
		
		String managerURI = "//"+Utils.getProp("manager_host")+":"+Utils.getProp("manager_port");
		while(manager == null)
		{
			try
			{
				manager = (SiteManager)WorkflowExecutor.getRemoteExecutor(managerURI);
			}
			catch (NotBoundException ex)
			{
				manager = null;
			}
		}
	}
	
	public static Worker get() 
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
	
	@Override
	public void submit(Workflow wf)  
	{
		//TODO: wait until all input files exist
		
		Schedule s = this.getScheduler().getSchedule(
				new SchedulerSettings(wf, execNetwork, this.getDefaultFC()));
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
		return null;
	}
	
	@Override
	public void dispatchTask()
	{
		while(workingProcessors < totalProcessors && !taskQueue.isEmpty())
		{
			final ScheduleEntry se = taskQueue.poll();
			if(se!=null)
			{
				new Thread(new Runnable() {

					@Override
					public void run()
					{
						processors[se.target.charAt(0)].exec(se.task);
					}
				}).start();
				workingProcessors++;
			}
		}
	}

	@Override
	public void setTaskStatus(TaskStatus status)  
	{
		//TODO: store task status to local db
		manager.setTaskStatus(status);
		if(status.status == TaskStatus.STATUS_COMPLETED)
		{
			workingProcessors--;
			dispatchTask();
		}
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
}
