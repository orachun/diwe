/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import workflowengine.utils.db.Cacher;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class ExecutorNetwork
{
	private Map<String, NetworkLink> workers = new HashMap<>();
	
	public void add(String weURI)
	{
		add(weURI, linkSpdTest(weURI), 0);
	}
	
	public void add(String weURI, double linkSpdMBps)
	{
		add(weURI, linkSpdMBps, 0);
	}
	public void add(String weURI, double linkSpdMBps, double linkUnitCost)
	{
		workers.put(weURI, new NetworkLink(linkSpdMBps, linkUnitCost));
	}
	public Set<String> getExecutorURISet()
	{
		return Collections.unmodifiableSet(workers.keySet());
	}
	public NetworkLink getLinkToWorker(String weURI)
	{
		return workers.get(weURI);
	}
	
	public double getTransferTime(String to, double sizeInMB)
	{
		return getLinkToWorker(to).getTransferTime(sizeInMB);
	}
	public double getTransferTime(String from, String to, double sizeInMB)
	{
		if(from.equals(to))
		{
			return 0;
		}
		double mbps = Math.min(getLinkToWorker(to).MBps, getLinkToWorker(from).MBps);
		return sizeInMB/mbps;
	}
	public double getTransferTime(String from, String to, Collection<WorkflowFile> wff)
	{
		double totalSize = 0;
		for(Object o : wff)
		{
			WorkflowFile f;
			if(o instanceof String)
			{
				f = (WorkflowFile)Cacher.get(WorkflowFile.class, o);
			}
			else
			{
				f = (WorkflowFile)o;
			}
			totalSize += f.getSize();
		}
		return getTransferTime(from, to, totalSize);
	}
	
	
	private double linkSpdTest(String to)
	{
		//TODO: implement this
		throw new UnsupportedOperationException("Not implemented yet.");
	}
}
