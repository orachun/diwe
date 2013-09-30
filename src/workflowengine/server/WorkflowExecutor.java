/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Set;
import workflowengine.communication.HostAddress;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.scheduler.Scheduler;
import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import workflowengine.utils.Logger;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public abstract class WorkflowExecutor extends UnicastRemoteObject implements WorkflowExecutorInterface
{
	protected HostAddress addr;
	protected String uri;
	protected Logger logger = Utils.getLogger();
	protected WorkflowExecutor() throws RemoteException
	{
		uri = "";
		addr = new HostAddress(Utils.getPROP(), "local_hostname", "local_port");
	}

	protected WorkflowExecutor(boolean registerForRMI, String name) throws RemoteException
	{
		this.uri = "//" + Utils.getProp("local_hostname") + "/" + name;
		if (registerForRMI)
		{
			try
			{
				System.out.println("Binding workflow executor to URI: " + uri);
				Naming.rebind(name, this);
			}
			catch (MalformedURLException e)
			{
				throw new RuntimeException(e.getMessage(), e);
			}
		}
		Utils.createDir(Utils.getProp("working_dir"));
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

	public static WorkflowExecutorInterface getRemoteExecutor(String weURI) throws NotBoundException
	{
		try
		{
			return (WorkflowExecutorInterface) Naming.lookup(weURI);
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
		}
		return null;
	}

	protected FC getDefaultFC()
	{
		return new MakespanFC();
	}

	public String getURI()
	{
		return uri;
	}

	public abstract RemoteWorker getWorker(String uri);

	public void greeting(String msg)
	{
		System.out.println(msg);
	}
	
	@Override
	public HostAddress getAddr()
	{
		return addr;
	}
}
