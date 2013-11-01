/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.Serializable;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class SuspendedTaskInfo implements Serializable
{
	public final String tid;
	public final WorkflowFile ckptFile;

	public SuspendedTaskInfo(String tid, WorkflowFile ckptFile)
	{
		this.tid = tid;
		this.ckptFile = ckptFile;
	}
	
}
