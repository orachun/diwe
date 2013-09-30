/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.Collection;
import java.util.Set;
import workflowengine.server.WorkflowExecutor;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class ImmutableExecutorNetwork extends ExecutorNetwork
{
	private ExecutorNetwork n;
	public ImmutableExecutorNetwork(ExecutorNetwork n)
	{
		this.n = n;
	}

	@Override
	public Set<String> getExecutorURISet()
	{
		return n.getExecutorURISet();
	}

	@Override
	public NetworkLink getLinkToWorker(String weURI)
	{
		return n.getLinkToWorker(weURI);
	}

	@Override
	public double getTransferTime(String to, double sizeInMB)
	{
		return n.getTransferTime(to, sizeInMB);
	}

	@Override
	public double getTransferTime(String from, String to, double sizeInMB)
	{
		return n.getTransferTime(from, to, sizeInMB);
	}

	@Override
	public double getTransferTime(String from, String to, Collection<WorkflowFile> wff)
	{
		return n.getTransferTime(from, to, wff);
	}


	@Override
	public void add(String weURI)
	{
		throw new UnsupportedOperationException("The object is immutable.");
	}

	@Override
	public void add(String weURI, double linkSpdMBps)
	{
		throw new UnsupportedOperationException("The object is immutable.");
	}

	@Override
	public void add(String weURI, double linkSpdMBps, double linkUnitCost)
	{
		throw new UnsupportedOperationException("The object is immutable.");
	}
	
}
