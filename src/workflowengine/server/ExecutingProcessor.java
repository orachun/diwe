/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.utils.Checkpointing;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Task;
import workflowengine.utils.Utils;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class ExecutingProcessor
{
	private final Worker manager;
	private Process currentProcess;
	private Task currentTask;
	private ScheduleEntry currentSE;
	private long firstSubmitTime = -1;
	private long usage = 0;
	private final Object SUSPEND_LOCK = new Object();
	private final Object IDLE_WAITING_LOCK = new Object();
	private boolean isSuspended = false;
	private Random r = new Random();
	private final int dmtcpPort;
	private TaskStatus ts = null;
	public ExecutingProcessor(int no, Worker manager) 
	{
		super();
		dmtcpPort = Utils.getIntProp("DMTCP_port")+no;
		this.manager = manager;
	}
	
	public void waitUntilIdle()
	{
		while(currentProcess != null)
		{
			synchronized(IDLE_WAITING_LOCK)
			{
				try
				{
					IDLE_WAITING_LOCK.wait(5000);
				}
				catch (InterruptedException ex)
				{}
			}
		}
	}
	
	public synchronized TaskStatus exec(Task t, ScheduleEntry se)
	{
		if(currentProcess != null)
		{
			manager.logger.log("ERROR: executing another process.");
			return null;
		}
		if(firstSubmitTime == -1)
		{
			firstSubmitTime = Utils.time();
		}
//		long start = Utils.time();
		this.currentTask = t;
		this.currentSE = se;
		ts = TaskStatus.executingStatus(se);
		int tries = 0;
		String[] cmds = prepareCmd(t, se);
		
		Checkpointing.startCoordinator(dmtcpPort);
		
		while (tries < 5)
		{
			ts = TaskStatus.executingStatus(se);
			try
			{
				synchronized (SUSPEND_LOCK)
				{
					isSuspended = false;
					currentProcess = startProcess(t, se, cmds);
					manager.setTaskStatus(ts);
				}
			}
			catch (IOException ex)
			{
				ts = ts.fail(-1, "Cannot start process: " + ex.getMessage());
				manager.logger.log("Cannot start process: " + ex.getMessage(), ex);
				currentProcess = null;
			}

			//If the process can be started
			if(currentProcess != null)
			{
				manager.logger.log("Task "+t.getName()+ " is started.");
//				manager.logger.log("CMD: "+t.getCmd());
				try
				{
					int ret = currentProcess.waitFor();
					
					synchronized(SUSPEND_LOCK)
					{

						if(isSuspended)
						{
							manager.logger.log("Task "+t.getName()+ " is suspended.");
							ts = ts.suspend();
							isSuspended = false;
							break;
						}
						
						if(ret == 0)
						{
							ts = ts.complete();
							manager.logger.log("Task "+t.getName()+ " is completed.");
							long delay = Math.random() > 0.3 ? 0 : (long)Math.abs(
									Math.round(Utils.getDoubleProp("percent_delay")
									*r.nextGaussian()*t.getEstimatedExecTime()));
							Thread.sleep(delay);
							break;
						}
						else
						{
							ts = ts.fail(ret, "Error: return value is "+ret+".");
							manager.logger.log("Error: Task "+t.getName()+ " returns "+ret+".");
							String dir = manager.getWorkingDir() + "/" + se.superWfid;
							Utils.printFileContent(dir + "/" + t.getName()+"_"+t.getUUID() + ".stdout");
							Utils.printFileContent(dir + "/" + t.getName()+"_"+t.getUUID() + ".stderr");
						}
					}
				}
				catch (InterruptedException ex)
				{
					ts = ts.fail(-1, "Waiting is interrupted before process ends.");
					manager.logger.log("Task "+t.getName()
							+ " is failed: Waiting is interrupted before process ends.");
				}
			}
			tries++;
		}
		
		if(ts.status != TaskStatus.STATUS_SUSPENDED)
		{
			manager.setTaskStatus(ts);
		}
		
		usage = Utils.time() - firstSubmitTime;
		currentProcess = null;
		currentTask = null;
		currentSE = null;
		ts = null;
		Checkpointing.stopCoordinator(dmtcpPort);
		synchronized(IDLE_WAITING_LOCK)
		{
			notify();
		}
		return ts;
	}

	public int getTotalProcessors()
	{
		return 1;
	}
	
	private String[] prepareCmd(Task t, ScheduleEntry se)
    {
		String taskDir = manager.getWorkingDir() + "/" + se.superWfid;
		String[] cmds;
		if(t.getStatus().status == TaskStatus.STATUS_SUSPENDED)
		{
			System.out.println("Restarting "+t.getName());
			WorkflowFile ckptFile = WorkflowFile.get(t.getCkptFid());
			String ckptFileName = taskDir+'/'+ckptFile.getName();
			Checkpointing.unpack(ckptFileName, taskDir);
			cmds = Checkpointing.getResumeCmd(taskDir, t.getUUID(), dmtcpPort).split(";");
		}
		else
		{
		String prefix;
			prefix = Checkpointing.getExecCmdPrefix(taskDir, t.getUUID(), dmtcpPort);
			cmds = (prefix	+taskDir + "/" + t.getCmd()).split(";");
		}
		
		for(String c : cmds)
		{
			System.out.printf("%s ", c);
		}
		System.out.println();
		
		return cmds;
    }
	
	private Process startProcess(Task t, ScheduleEntry se, String[] cmds) throws IOException
    {
		String dir = manager.getWorkingDir() + "/" + se.superWfid;
		boolean isResume = t.getStatus().status == TaskStatus.STATUS_SUSPENDED;
        ProcessBuilder pb = Utils.createProcessBuilder(
				cmds,
                dir,
				isResume ? null : dir + "/" + t.getName()+"_"+t.getUUID() + ".stdout",
				isResume ? null : dir + "/" + t.getName()+"_"+t.getUUID() + ".stderr", 
				null);
        currentProcess = pb.start();
		return currentProcess;
    }

	public void stop()
	{
		currentProcess.destroy();
	}

	public long getUsage()
	{
		return usage;
	}

	private Set<String> getRelatedOutputFiles(Task t)
	{
		Set<String> files = new HashSet<>();
		String taskDir = manager.getWorkingDir() + '/'+t.getSuperWfid() + '/';
		for(String fid : t.getOutputFiles())
		{
			String fpath = taskDir+WorkflowFile.get(fid).getName();
			
			if(Utils.fileExists(fpath))
			{
				files.add(fpath);
			}
		}
		files.add(taskDir+t.getName()+"_"+t.getUUID() + ".stdout");
		files.add(taskDir+t.getName()+"_"+t.getUUID() + ".stderr");
		return files;
	}
	
	public SuspendedTaskInfo suspend()
	{
		if(currentProcess != null)
		{
			synchronized(this.manager)
			{
				synchronized(SUSPEND_LOCK)
				{
					if(isSuspended || Utils.isProcTerminated(currentProcess))
					{
						return null;
					}
					
					//Not suspend if the task almost finishes
					if((Utils.time()-ts.start)/currentTask.getEstimatedExecTime() > 0.8)
					{
						return null;
					}
					System.out.println("Suspending current task...");
					if(Checkpointing.checkpoint(dmtcpPort) != 0)
					{
						return null;
					}
					isSuspended = true;
					currentProcess.destroy();

					String tid = currentTask.getUUID();
					String taskDir = manager.getWorkingDir() + '/' + this.currentSE.superWfid;
					String ckptDir = Checkpointing.getCkptDir(taskDir, tid);
					String ckptFileName = taskDir + '/' + tid + "_ckpt.tar";
					Checkpointing.pack(ckptFileName, ckptDir, getRelatedOutputFiles(currentTask));

					WorkflowFile ckptFile = new WorkflowFile(tid + "_ckpt.tar", 
							Utils.getFileLength(ckptFileName), 
							WorkflowFile.TYPE_CHECKPOINT_FILE, 
							Utils.uuid()
							);

					System.out.println("Done.");
					ts = ts.suspend();
//					currentTask.setStatus(ts);
					manager.setTaskStatus(ts);
					return new SuspendedTaskInfo(tid, ckptFile);
				}
			}
		}
		
		return null;
	}
	
	
	public void shutdown()
	{
		Checkpointing.stopCoordinator(dmtcpPort);
	}
}