/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lipermi.net.Client;
import workflowengine.communication.HostAddress;
import workflowengine.monitor.HTMLUtils;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.scheduler.Scheduler;
import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import workflowengine.server.filemanager.FileManager;
import workflowengine.utils.Logger;
import workflowengine.utils.SystemStats;
import workflowengine.utils.Utils;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.DBRecord;
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
	protected static WorkflowExecutor instant;
	protected WorkflowExecutorInterface manager;
	protected String managerURI;
	protected HostAddress addr;
	protected String uri;
	public Logger logger = Utils.getLogger();
	protected int totalProcessors = 0;
	protected double avgBandwidth = -1;
//	protected Set<String> notFinishedWorkflows = new HashSet<>();
	protected WorkflowExecutor()  //throws RemoteException
	{
		uri = "";
		addr = new HostAddress(Utils.getPROP(), "local_hostname", "local_port");
	}

	protected WorkflowExecutor(boolean registerForRMI, String name)  //throws RemoteException
	{
		Utils.mkdirs(Utils.getProp("working_dir"));
		instant = this;
		addr = new HostAddress(Utils.getPROP(), "local_hostname", "local_port");
		this.uri = Utils.getProp("local_hostname")+":"+Utils.getIntProp("local_port");
		DBRecord.prepareConnection();
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
		for(DBRecord r : DBRecord.select(
				"select tid, wkid from schedule "))
		{
			mapping.put(r.get("tid"), r.get("wkid"));
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
		return sb.toString();
	}
	
	
	@Override
	public void setTaskStatus(TaskStatus status)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public String getManagerURI()  //throws RemoteException
	{
		return managerURI;
	}

	@Override
	public void submit(Workflow wf, Properties prop)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
			String workingDir = getWorkingDir()+"/"+wf.getSuperWfid();
			Utils.mkdirs(workingDir);
			Utils.cp(input_dir+"/*", workingDir);
			submit(wf, prop);
		}
		catch (IOException ex)
		{
//			throw new RemoteException(ex.getMessage(), ex);
		}
	}
	
	
	
	
	
	@Override
	public void shutdown()
	{
		System.out.println("Shutting down...");
		Set<String> workers = getWorkerSet();
		if(workers != null)
		{
			for(String w : getWorkerSet())
			{
				try
				{
					getRemoteExecutor(w).shutdown();
				}
				catch (Exception ex)
				{
					logger.log("Cannot tell worker "+w+" to shutdown.", ex);
				}
			}
		}
		
		new Thread(){

			@Override
			public void run()
			{
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException ex)
				{}
				Cacher.saveAll();
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
	
	
	
	
	
	
	
	
	
	
	
	
	
	// <editor-fold defaultstate="collapsed" desc="Not implemented methods">

	
	
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
	public String getTaskQueueHTML()  //throws RemoteException
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

	@Override
	public String getWorkingDir()  //throws RemoteException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	// </editor-fold>

	
	
}
