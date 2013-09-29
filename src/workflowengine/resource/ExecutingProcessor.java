/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Set;
import workflowengine.WorkflowExecutor;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.Worker;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class ExecutingProcessor extends WorkflowExecutor
{
	private Worker manager;
	public ExecutingProcessor(Worker manager) throws RemoteException
	{
		super(false);
		this.manager = manager;
	}
	
	public TaskStatus exec(Task t)
	{
		TaskStatus ts = TaskStatus.executingStatus(t.getUUID());
		manager.setTaskStatus(ts);
		Process p;
		try
		{
			p = startProcess(t);
		}
		catch (IOException ex)
		{
			manager.setTaskStatus(ts.fail(-1, "Cannot start process: "+ex.getMessage()));
			p = null;
		}
		
		if(p != null)
		{
			try
			{
				int ret = p.waitFor();
				if(ret == 0)
				{
					ts = ts.complete();
					manager.setTaskStatus(ts);
				}
				else
				{
					ts = ts.fail(ret, "Unknown error: return value is not 0.");
					manager.setTaskStatus(ts);
				}
			}
			catch (InterruptedException ex)
			{
				ts = ts.fail(-1, "Waiting is interrupted before process ends.");
				manager.setTaskStatus(ts);
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
        return t.getCmd().split(";");
    }
	
	private Process startProcess(Task t) throws IOException
    {
        ProcessBuilder pb = Utils.createProcessBuilder(prepareCmd(t),
                manager.getWorkingDir(),
                manager.getWorkingDir() + t.getUUID() + ".stdout",
                manager.getWorkingDir() + t.getUUID() + ".stderr", "");
        return pb.start();
    }
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	@Override
	public RemoteWorker getWorker(String uri)
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}
	
	@Override
	public void registerWorker(String uri, int totalProcessors)
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}
	
	@Override
	public Set<String> getExecutorURIs()
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void dispatchTask()
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void submit(Workflow wf)
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void setTaskStatus(TaskStatus status)
	{
		throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
	}

	
	
}
