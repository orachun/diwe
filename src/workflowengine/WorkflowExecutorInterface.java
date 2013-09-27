/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public interface WorkflowExecutorInterface
{
	public void submit(Workflow wf);
	public int getTotalProcessors();
	public void setTaskStatus(TaskStatus status);
	public void registerWorker(String uri, int totalProcessors);
}
