/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import workflowengine.server.ExecutingProcessor;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.server.WorkflowExecutor;
import workflowengine.server.WorkflowExecutorInterface;

/**
 *
 * @author orachun
 */
public class RemoteWorker
{
	private String URI;
	private WorkflowExecutorInterface worker;
	private int totalProcessors;
	
	public RemoteWorker(String URI) throws NotBoundException
	{
		this.URI = URI;
		this.worker = WorkflowExecutor.getRemoteExecutor(URI);
		try
		{
			totalProcessors = worker.getTotalProcessors();
		}
		catch (RemoteException ex)
		{
			Logger.getLogger(RemoteWorker.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public RemoteWorker(String URI, ExecutingProcessor p)
	{
		this.URI = URI;
		this.worker = p;
	}

	public String getURI()
	{
		return URI;
	}

	public WorkflowExecutorInterface getWorker()
	{
		return worker;
	}

	public int getTotalProcessors()
	{
		return totalProcessors;
	}
	
	
}
