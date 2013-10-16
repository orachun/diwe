/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import workflowengine.server.ExecutingProcessor;
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
	
	public RemoteWorker(String URI, int processors)
	{
		this.URI = URI;
		totalProcessors = processors;
		this.worker = WorkflowExecutor.getRemoteExecutor(URI);
//		try
//		{
//			totalProcessors = worker.getTotalProcessors();
//		}
//		catch (RemoteException ex)
//		{
//			Logger.getLogger(RemoteWorker.class.getName()).log(Level.SEVERE, null, ex);
//		}
	}
	
	
	
	public RemoteWorker(String URI, ExecutingProcessor p)
	{
		this.URI = URI;
		this.worker = new ProcessorWrapper(p);
	}
	
	private static class ProcessorWrapper extends WorkflowExecutor
	{
		private ExecutingProcessor p;

		public ProcessorWrapper(ExecutingProcessor p)
		{
			this.p = p;
		}

		@Override
		public int getTotalProcessors()
		{
			return p.getTotalProcessors();
		}
		
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
	
	public void setTotalProcessors(int p)
	{
		this.totalProcessors = p;
	}
	
}
