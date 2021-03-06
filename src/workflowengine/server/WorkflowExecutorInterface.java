/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.rmi.Remote;
import java.util.Set;
import workflowengine.monitor.EventLogger;
import workflowengine.utils.HostAddress;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Workflow;

/**
 *
 * @author orachun
 */
public interface WorkflowExecutorInterface extends Remote 
{
	public void submit(String daxFile, java.util.Properties prop) ; //throws RemoteException;
	public void submit(Workflow wf, java.util.Properties prop) ; //throws RemoteException;

	public int getTotalProcessors() ; //throws RemoteException;

	public void setTaskStatus(TaskStatus status); //throws RemoteException;

	public void registerWorker(String uri, int totalProcessors); //throws RemoteException;

	public void greeting(String msg) ; //throws RemoteException;

	public void stop() ; //throws RemoteException;
	
	public HostAddress getAddr() ; //throws RemoteException;
	
	public String getWorkingDir() ; //throws RemoteException;
	
	public void shutdown() ; //throws RemoteException;
	
	
	public double getAvgBandwidth();
	public Set<SuspendedTaskInfo> suspendRunningTasks();
	public void removeWorkflowFromQueue(String superWfid);
	
	//For monitoring tool
	public String getTaskQueueHTML() ; //throws RemoteException;
	public String getTaskMappingHTML() ; //throws RemoteException;
	public String getManagerURI() ; //throws RemoteException;
	public Set<String> getWorkerSet() ; //throws RemoteException;
	public String getStatusHTML() ; //throws RemoteException;
	public long getUsage();
	public double getTotalCost();
	public long getTransferredBytes();
	public EventLogger getEventLog();
	
	//For debugging only
	public String exec(String cmd) ; //throws RemoteException;
	
}
