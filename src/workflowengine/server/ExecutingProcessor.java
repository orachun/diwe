/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.IOException;
import workflowengine.schedule.ScheduleEntry;
import workflowengine.server.filemanager.FileServer;
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
	private Worker manager;
	private Process currentProcess;
	private Task currentTask;
	private ScheduleEntry currentSE;
	private long usage = 0;
	private final Object SUSPEND_LOCK = new Object();
	private boolean isSuspended = false;
	public ExecutingProcessor(Worker manager) 
	{
		super();
		this.manager = manager;
	}
	
	public synchronized TaskStatus exec(Task t, ScheduleEntry se)
	{
		if(currentProcess != null)
		{
			throw new IllegalStateException("Executing other process.");
		}
		long start = Utils.time();
		this.currentTask = t;
		this.currentSE = se;
		TaskStatus ts = TaskStatus.executingStatus(se);
		int tries = 0;
		while (ts.status != TaskStatus.STATUS_COMPLETED && tries < 5)
		{
			try
			{
				synchronized (SUSPEND_LOCK)
				{
					isSuspended = false;
					currentProcess = startProcess(t, se);
				}
				manager.setTaskStatus(ts);
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
						}
						else
						{
							ts = ts.fail(ret, "Error: return value is not 0.");
							manager.logger.log("Error: Task "+t.getName()+ " did not return 0.");
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
		
		manager.setTaskStatus(ts);
		
		usage += (Utils.time() - start);
		currentProcess = null;
		currentTask = null;
		currentSE = null;
		return ts;
	}

	public int getTotalProcessors()
	{
		return 1;
	}
	
	private String[] prepareCmd(Task t, ScheduleEntry se)
    {
		String taskDir = manager.getWorkingDir() + "/" + se.superWfid;
		String prefix;
		if(t.getStatus().status == TaskStatus.STATUS_SUSPENDED)
		{
			prefix = Checkpointing.getResumeCmdPrefix(taskDir, t.getUUID());
			WorkflowFile ckptFile = WorkflowFile.get(t.getCkptFid());
			Utils.bash("tar -xzf "+taskDir+'/'+ckptFile.getName()+" -C "+taskDir, false);
		}
		else
		{
			prefix = Checkpointing.getExecCmdPrefix(taskDir, t.getUUID());
		}
		String[] cmds = (prefix	+taskDir + "/" + t.getCmd()).split(";");
		
//		for(String c : cmds)
//		{
//			System.out.printf("%s ", c);
//		}
//		System.out.println();
		
		return cmds;
    }
	
	private Process startProcess(Task t, ScheduleEntry se) throws IOException
    {
		String dir = manager.getWorkingDir() + "/" + se.superWfid;
        ProcessBuilder pb = Utils.createProcessBuilder(
				prepareCmd(t, se),
                dir,
				dir + "/" + t.getName()+"_"+t.getUUID() + ".stdout",
				dir + "/" + t.getName()+"_"+t.getUUID() + ".stderr", null);
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

	public WorkflowFile suspend(String tid)
	{
		if(currentProcess != null && currentTask.getUUID().equals(tid))
		{
			synchronized(SUSPEND_LOCK)
			{
				System.out.println("Suspending current task...");
				if(isSuspended)
				{
					return null;
				}
				isSuspended = true;
				Checkpointing.checkpoint();
				
				currentProcess.destroy();
				
				String taskDir = manager.getWorkingDir() + '/' + this.currentSE.superWfid;
				String ckptDir = Checkpointing.getCkptDir(taskDir, tid);
				String ckptFileName = taskDir + '/' + tid + "_ckpt.tar";
				Utils.bash("tar -zcf " + ckptFileName + ' ' + ckptDir, false);

				WorkflowFile ckptFile = new WorkflowFile(tid + "_ckpt.tar", 
						Utils.getFileLength(ckptFileName), 
						WorkflowFile.TYPE_CHECKPOINT_FILE, 
						Utils.uuid()
						);
				System.out.println("Done.");
				return ckptFile;
			}
		}
		
		return null;
	}
	
}
