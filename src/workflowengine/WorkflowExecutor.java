/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import removed.TaskManager;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;
import java.util.logging.Level;
import workflowengine.communication.HostAddress;
import workflowengine.schedule.Scheduler;
import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public abstract class WorkflowExecutor extends UnicastRemoteObject implements WorkflowExecutorInterface
{
	private HostAddress host;
	protected WorkflowExecutor() throws RemoteException
	{
		this(true);
	}
	protected WorkflowExecutor(boolean registerForRMI) throws RemoteException
	{
		if(registerForRMI)
		{
			try
			{
				Naming.rebind("WorkflowExecutor", this);
			}
			catch (MalformedURLException e)
			{
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}
	public abstract Set<String> getExecutorURIs();
	protected void schedule(Workflow wf)
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}
	public Task getNextReadyTask()
	{
		throw new UnsupportedOperationException("Not implemented yet");
	}
	public abstract void dispatchTask();
	public static WorkflowExecutor getRemoteExecutor(String weURI) throws NotBoundException
	{
		try
		{
			return (WorkflowExecutor)Naming.lookup(weURI);
		}
		catch (MalformedURLException | RemoteException e)
		{
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	protected Scheduler getScheduler() 
    {
        try
        {
            Class c = ClassLoader.getSystemClassLoader().loadClass(Utils.getProp("scheduler").trim());
            Scheduler s = (Scheduler) c.newInstance();
            return s;
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex)
        {
            java.util.logging.Logger.getLogger(TaskManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
	
	protected FC getDefaultFC()
	{
		return new MakespanFC();
	}
	
	public String getURI()
	{
		return "//"+host.toString()+"/WorkflowExecutor";
	}
}
