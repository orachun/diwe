/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.resource;

import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author Orachun
 */
public class NetworkLink
{
    public final double MBps; //MBps
	public final double unitCost; //Cost per MB

	public NetworkLink(double MBps)
	{
		this(MBps, 0);
	}
	
    public NetworkLink(double MBps, double unitCost)
    {
        this.MBps = MBps;
		this.unitCost = unitCost;
    }
	
	public double getTransferTime(double fileSizeInBytes)
	{
		return fileSizeInBytes/1024.0/1024.0/(MBps);
	}
	public double getTransferTime(WorkflowFile wff)
	{
		return getTransferTime(wff.getSize());
	}
	public double getTransferTime(WorkflowFile[] wff)
	{
		double totalSize = 0;
		for(WorkflowFile f : wff)
		{
			totalSize += f.getSize();
		}
		return getTransferTime(totalSize);
	}
	public double getTransferCost(double fileSizeInMB)
	{
		return fileSizeInMB/unitCost;
	}
	public double getTransferCost(WorkflowFile wff)
	{
		return getTransferCost(wff.getSize());
	}
	public double getTransferCost(WorkflowFile[] wff)
	{
		double totalSize = 0;
		for(WorkflowFile f : wff)
		{
			totalSize += f.getSize();
		}
		return getTransferCost(totalSize);
	}
}
