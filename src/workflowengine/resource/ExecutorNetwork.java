/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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
		return new HashSet<>(workers.keySet());
	}
	public NetworkLink getLinkToWorker(String weURI)
	{
		return workers.get(weURI);
	}
	
	
	public double getTransferTime(String worker, double sizeInBytes)
	{
		return getLinkToWorker(worker).getTransferTime(sizeInBytes);
	}
	
	public double getTransferTime(String worker, Collection wff)
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
		return getTransferTime(worker, totalSize);
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
	
	
	public double getTransferTime(String from, String to, Collection wff)
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
//		String remoteHost = to.split(":")[0];
//		double total = 0;
//		Utils.bash("truncate --size 1M 1mb.dat", true);
//		for(int i=0;i<1;i++)
//		{
//			long startTime = Utils.timeMillis();
//			Utils.bash("nc "+remoteHost+" 19191 < 1mb.dat", true);
//			total += 102400000.0/(Utils.timeMillis()-startTime);
//		}
//		return total/1.0;
		return 1.7;
	}
	
	public double getLinkSpd(String to)
	{
		return workers.get(to).MBps;
	}
}
