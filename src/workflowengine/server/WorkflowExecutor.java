/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import com.mongodb.BasicDBList;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lipermi.net.Client;
import workflowengine.utils.HostAddress;
import workflowengine.monitor.EventLogger;
import workflowengine.monitor.HTMLUtils;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.scheduler.Scheduler;
import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import workflowengine.server.filemanager.FileManager;
import workflowengine.server.filemanager.FileServer;
import workflowengine.utils.Logger;
import workflowengine.utils.SystemStats;
import workflowengine.utils.Utils;
import workflowengine.utils.db.MongoDB;
import workflowengine.workflow.Task;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFactory;

/**
 *
 * @author orachun
 */
public abstract class WorkflowExecutor implements WorkflowExecutorInterface
{
	public static final char FROM_WORKER = 'W';
	public static final char FROM_MANAGER = 'M';
	public static final char FROM_SELF = 'S';
	public static final double COST_PER_SECOND = 0.000022222;
	public static final double COST_PER_BYTE =   0.000000000005;
	
	protected static WorkflowExecutor instant;
	protected WorkflowExecutorInterface manager;
	protected String managerURI;
	protected HostAddress addr;
	protected String uri;
	public Logger logger = Utils.getLogger();
	public EventLogger eventLogger;
	protected int totalProcessors = 0;
	protected double avgBandwidth = -1;
	private String workingDir = Utils.getProp("working_dir");
	protected TaskQueue taskQueue = new TaskQueue();
//	protected Set<String> notFinishedWorkflows = new HashSet<>();
	protected WorkflowExecutor()  //throws RemoteException
	{
		uri = "";
		addr = new HostAddress(Utils.getPROP(), "local_hostname", "local_port");
	}

	protected WorkflowExecutor(boolean registerForRMI, String name)  //throws RemoteException
	{

		eventLogger = new EventLogger();
		eventLogger.start("EXEC_INIT", "Initializing executor");
		Utils.mkdirs(Utils.getProp("working_dir"));
		instant = this;
		addr = new HostAddress(Utils.getPROP(), "local_hostname", "local_port");
		this.uri = Utils.getProp("local_hostname")+":"+Utils.getIntProp("local_port");
		
		//Start default file server
//		try
//		{
//			FileServer.get(workingDir);
//		}
//		catch (IOException ex)
//		{
//			logger.log("Cannot start file server.", ex);
//			shutdown();
//		}
		
		//Prepare Mongo Database
		if(!MongoDB.prepare())
		{
			logger.log("Cannot prepare database connection.");
			shutdown();
		}
		
		if (registerForRMI)
		{			
			Utils.registerRMIServer(WorkflowExecutorInterface.class, this);
			boolean hasManager = Utils.hasProp("manager_host")
					&& !Utils.getProp("manager_host").isEmpty()
					&& Utils.hasProp("manager_port")
					&& !Utils.getProp("manager_port").isEmpty();
			if (hasManager)
			{
				managerURI = Utils.getProp("manager_host")+":"+Utils.getProp("manager_port");
				while (manager == null)
				{
					manager = (WorkflowExecutorInterface) WorkflowExecutor
							.getRemoteExecutor(managerURI);
				}
			}
			System.out.println("Done.");
			System.out.println("Initializing file manager...");
			FileManager.get();
			
			if (hasManager)
			{
				System.out.println("Registering to manager...");
				manager.registerWorker(uri, totalProcessors);
				manager.greeting("Hello from " + uri);
			}
			System.out.println("Done.");
		}
		eventLogger.finish("EXEC_INIT");
		
		
		
		new Thread()
		{
			@Override
			public void run()
			{
				while(true)
				{
					try{
						Thread.sleep(1000);
					}
					catch(Exception e)
					{}
					dispatchTask();
				}
			}			
		}.start();
	}
	
	public static WorkflowExecutor get()
	{
		return instant;
	}

	
	public static WorkflowExecutorInterface getRemoteExecutor(String weURI)
	{
		WorkflowExecutorInterface worker = null;
		int tries = 0;
		while(worker == null && tries < 10)
		{
			try
			{
				Client c = Utils.getRMIClient(weURI);
				worker = (WorkflowExecutorInterface) c.getGlobal(WorkflowExecutorInterface.class);
			}
			catch (Exception e)
			{
				worker = null;
				tries++;
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException ex)
				{}
			}
		}
		return worker;
	}
	
	/**
	 * Get a WorkflowExecutor who manage this local site
	 * @return 
	 */
	public static WorkflowExecutorInterface getSiteManager()
	{
		try
		{
			return getRemoteExecutor(Utils.getProp("manager_host")+":"+Utils.getIntProp("manager_port"));
		}
		catch(NullPointerException e)
		{
			return null;
		}
	}

	protected Scheduler getScheduler()
	{
		try
		{
			Class c = ClassLoader.getSystemClassLoader().loadClass(Utils.getProp("scheduler").trim());
			Scheduler s = (Scheduler) c.newInstance();
			return s;
//			return new CircularScheduler();
		}
		catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex)
		{
			logger.log("Cannot get a scheduler.", ex);
		}
		return null;
	}

	protected FC getDefaultFC()
	{
		return new MakespanFC();
//		return new CostOptimizationFC();
	}

	public String getURI()
	{
		return uri;
	}


	@Override
	public void greeting(String msg)
	{
		System.out.println(msg);
	}
	
	@Override
	public HostAddress getAddr()
	{
		return addr;
	}


	@Override
	public String getTaskMappingHTML()  //throws RemoteException
	{
		HashMap<String, String> mapping = new HashMap<>();
//		for(DBRecord r : DBRecord.select(
//				"select tid, wkid from schedule "))
//		{
//			mapping.put(r.get("tid"), r.get("wkid"));
//		}
		DBCursor cursor = MongoDB.SCHEDULE.find();
		while(cursor.hasNext())
		{
			DBObject o = cursor.next();
			BasicDBList list = (BasicDBList)o.get("mapping");
			for(Object lo : list)
			{
				DBObject obj = (DBObject)lo;
				mapping.put((String)obj.get("tid"), (String)obj.get("wkid"));
			}
		}
		
		
		
		StringBuilder mappingHTML = new StringBuilder();
		for(Map.Entry<String, String> entry : mapping.entrySet())
		{
			String tid = entry.getKey();
			String wkid = entry.getValue();
			Task t = Task.get(tid);
			if(t.getStatus().status == TaskStatus.STATUS_COMPLETED)
			{
				mappingHTML.append("<div class=\"task-mapping-entry\">[")
					.append(t.getStatus().status)
					.append("]")
					.append(t.getName())
					.append(":")
					.append(wkid)
					.append("</div>");
			}
		}
		for(Map.Entry<String, String> entry : mapping.entrySet())
		{
			String tid = entry.getKey();
			String wkid = entry.getValue();
			Task t = Task.get(tid);
			if(t.getStatus().status != TaskStatus.STATUS_COMPLETED)
			{
				mappingHTML.append("<div class=\"task-status-entry\">[[")
					.append(t.getStatus().status)
					.append("]]")
					.append(t.getName())
					.append(":")
					.append(wkid)
					.append("</div>");
			}
		}
		return mappingHTML.toString();
	}
	
	@Override
	public String getStatusHTML()  //throws RemoteException
	{
		StringBuilder sb = new StringBuilder();
		sb.append("<h1>System Status</h1>").append(HTMLUtils.nl2br(SystemStats.getStat()));
		sb.append("<br/>").append("Total Processors: ").append(this.totalProcessors);
		sb.append("<br/>").append("Manager URI: ").append(this.managerURI);
		sb.append("<br/>").append("Average Bandwidth: ").append(getAvgBandwidth());
		sb.append("<br/>").append("Exec Usage(s): ").append(getUsage());
		sb.append("<br/>").append("Transferred Data(bytes): ").append(getTransferredBytes());
		
		sb.append("<h1>Workers</h1>");
		Set<String> workerSet = getWorkerSet();
		if(workerSet != null)
		{
			sb.append("<ul>");
			for(String w : workerSet)
			{
				sb.append("<li>").append(w).append("</li>");
			}
			sb.append("</ul>");
		}
		sb.append("<br/>").append(eventLogger.toHTML());
		
//		if(FileManager.get() instanceof DIFileManager)
//		{
//			sb.append("<h1>Sent File Pieces</h1>")
//					.append(((DIFileManager)FileManager.get()).getSentPieceHTML())
//					.append("<h1>Received File Pieces</h1>")
//					.append(((DIFileManager)FileManager.get()).getReceivePieceHTML())
//					;
//		}
		
		return sb.toString();
	}
	
	
	@Override
	public String getManagerURI()  //throws RemoteException
	{
		return managerURI;
	}


	@Override
	public void submit(String dax, Properties prop)  //throws RemoteException
	{
		try
		{
			File f = File.createTempFile("dax", ".daxtmp");
			FileWriter fw = new FileWriter(f);
			fw.append(dax);
			fw.close();
			Workflow wf;
			String name = prop.getProperty("dax_file");
			if(name.endsWith(".dummy"))
			{
				wf = WorkflowFactory.fromDummyDAX(f.getAbsolutePath(), name);			
				wf.isDummy = true;
			}
			else
			{
				wf = WorkflowFactory.fromDAX(f.getAbsolutePath(), name);	
			}
			String input_dir = prop.getProperty("input_dir");
			String workflowDir = getWorkingDir()+"/"+wf.getSuperWfid();
			Utils.mkdirs(workflowDir);
			Utils.cp(input_dir+"/*", workflowDir);
			submit(wf, prop);
		}
		catch (IOException ex)
		{
			logger.log("Cannot submit the workflow.", ex);
		}
	}
	
	
	
	
	
	@Override
	public void shutdown()
	{
		final WorkflowExecutor thisSite = this;
		new Thread()
		{
			@Override
			public void run()
			{
				System.out.println("Shutting down...");
				FileManager.get().shutdown();
				Set<String> workers = getWorkerSet();
				if ((thisSite instanceof SiteManager) && workers != null)
				{
					for (String w : getWorkerSet())
					{
						try
						{
							getRemoteExecutor(w).shutdown();
						}
						catch (Exception ex)
						{
							logger.log("Cannot tell worker " + w + " to shutdown.", ex);
						}
					}
				}
//				Cacher.saveAll();
//				Checkpointing.stopCoordinator();
				System.out.println("Done.");
				System.exit(0);
			}
		}.start();
	}

	
	/**
	 * For debugging only
	 * @deprecated 
	 * @param cmd
	 * @return
	 * @ //throws RemoteException 
	 */
	@Override
	public String exec(String cmd)  //throws RemoteException
	{
		return Utils.execAndWait(new String[]{
			"bash", "-c", cmd
		}, true);
	}

	@Override
	public EventLogger getEventLog()
	{
		return eventLogger;
	}

	
	@Override
	public String getTaskQueueHTML()
	{
		return taskQueue.toHTML();
	}
	
	
	
	@Override
	public String getWorkingDir()
	{
		return workingDir;
	}

	
	
	@Override
	public void removeWorkflowFromQueue(String superWfid)
	{
		taskQueue.removeWorkflow(superWfid);
		for(String worker : getWorkerSet())
		{
			getRemoteExecutor(worker).removeWorkflowFromQueue(superWfid);
		}
	}
	
	
	public void logTaskStatus(TaskStatus status)
	{
		Task t = Task.get(status.taskID);
		switch(status.status)
		{
			case TaskStatus.STATUS_DISPATCHED:
//				eventLogger.event("DISPATCH: " + t.getName(), "");
				break;
			case TaskStatus.STATUS_EXECUTING:
				eventLogger.start("EXEC: " + t.getName(), "");
				break;
			case TaskStatus.STATUS_FAIL:
				eventLogger.finish("EXEC: " + t.getName());
				break;
			case TaskStatus.STATUS_COMPLETED:
				eventLogger.finish("EXEC: " + t.getName());
				break;
			case TaskStatus.STATUS_SUSPENDED:
				eventLogger.finish("EXEC: " + t.getName());
				eventLogger.event("SUSP: "+t.getName(), "");
				break;
		}
	}
	
	public double getTotalCost()
	{
		throw new RuntimeException("not implemented yet");
	}
	
	
	
	
	
	// <editor-fold defaultstate="collapsed" desc="Not implemented methods">

	
	@Override
	public void setTaskStatus(TaskStatus status)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void submit(Workflow wf, Properties prop)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	

	@Override
	public long getTransferredBytes()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public long getUsage()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public Set<SuspendedTaskInfo> suspendRunningTasks()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	
	@Override
	public double getAvgBandwidth()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	
	@Override
	public Set<String> getWorkerSet()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	
	public void dispatchTask()
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	
	
	public RemoteWorker getWorker(String uri)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	

	@Override
	public int getTotalProcessors()  //throws RemoteException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	@Override
	public void registerWorker(String uri, int totalProcessors)  //throws RemoteException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void stop()  //throws RemoteException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}



	// </editor-fold>

	
	
}
