/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.IOException;
import java.rmi.RemoteException;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Task;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class ExecutingProcessor extends WorkflowExecutor
{
	private Worker manager;
	private Process currentProcess;
	public ExecutingProcessor(Worker manager) throws RemoteException
	{
		super();
		this.manager = manager;
	}
	
	public TaskStatus exec(Task t)
	{
		if(currentProcess != null)
		{
			throw new IllegalStateException("Executing other process.");
		}
		TaskStatus ts = TaskStatus.executingStatus(t.getUUID());
		Process p;
		try
		{
			p = startProcess(t);
		}
		catch (IOException ex)
		{
			manager.setTaskStatus(ts.fail(-1, "Cannot start process: "+ex.getMessage()));
			manager.logger.log("Cannot start proceess.", ex);
			p = null;
		}
		manager.setTaskStatus(ts);
		
		if(p != null)
		{
			manager.logger.log("Task "+t.getName()+ " is started.");
			manager.logger.log("CMD: "+t.getCmd());
			try
			{
				int ret = p.waitFor();
				currentProcess = null;
				if(ret == 0)
				{
					ts = ts.complete();
					manager.setTaskStatus(ts);
					manager.logger.log("Task "+t.getUUID()+ " is completed.");
				}
				else
				{
					ts = ts.fail(ret, "Unknown error: return value is not 0.");
					manager.setTaskStatus(ts);
					manager.logger.log("Task "+t.getUUID()+ " is failed: return value is not 0.");
				}
			}
			catch (InterruptedException ex)
			{
				ts = ts.fail(-1, "Waiting is interrupted before process ends.");
				manager.setTaskStatus(ts);
				manager.logger.log("Task "+t.getUUID()+ " is failed: Waiting is interrupted before process ends.");
			}
		}
		return ts;
	}

	@Override
	public int getTotalProcessors()
	{
		return 1;
	}
	
	private String[] prepareCmd(Task t)
    {
        String[] cmds = t.getCmd().split(";");
		cmds[0] = Utils.getProp("working_dir")+"/"+cmds[0];
		return cmds;
    }
	
	private Process startProcess(Task t) throws IOException
    {
        ProcessBuilder pb = Utils.createProcessBuilder(
				prepareCmd(t),
                manager.getWorkingDir(),
				manager.getWorkingDir() + "/" + t.getUUID() + ".stdout",
				manager.getWorkingDir() + "/" + t.getUUID() + ".stderr", null);
        currentProcess = pb.start();
		return currentProcess;
    }

	@Override
	public void stop() throws RemoteException
	{
		currentProcess.destroy();
	}

	
	
}
