/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.rmi.Remote;
import java.rmi.RemoteException;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public interface WorkflowExecutorInterface extends Remote 
{
	public void submit(Workflow wf) throws RemoteException;

	public int getTotalProcessors() throws RemoteException;

	public void setTaskStatus(TaskStatus status) throws RemoteException;

	public void registerWorker(String uri, int totalProcessors) throws RemoteException;

	public void greeting(String msg) throws RemoteException;

	public void stop() throws RemoteException;
}
