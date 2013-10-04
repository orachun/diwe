/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lipermi.exception.LipeRMIException;
import lipermi.handler.CallHandler;
import lipermi.handler.filter.GZipFilter;
import lipermi.net.Client;
import lipermi.net.IClientListener;
import lipermi.net.IServerListener;
import lipermi.net.Server;
import workflowengine.communication.HostAddress;
import workflowengine.monitor.HTMLUtils;
import workflowengine.resource.RemoteWorker;
import workflowengine.schedule.scheduler.Scheduler;
import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import workflowengine.utils.Logger;
import workflowengine.utils.SystemStats;
import workflowengine.utils.Utils;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.DBRecord;
import workflowengine.workflow.Task;
import workflowengine.workflow.TaskRanker;
import workflowengine.workflow.TaskStatus;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFactory;

/**
 *
 * @author orachun
 */
public abstract class WorkflowExecutor implements WorkflowExecutorInterface
{
	protected WorkflowExecutorInterface manager;
	protected String managerURI;
	protected HostAddress addr;
	protected String uri;
	protected Logger logger = Utils.getLogger();
	protected int totalProcessors = 0;
//	protected Set<String> notFinishedWorkflows = new HashSet<>();
	protected WorkflowExecutor()  //throws RemoteException
	{
		uri = "";
		addr = new HostAddress(Utils.getPROP(), "local_hostname", "local_port");
		
	}

	private CallHandler callHandler = new CallHandler();
	protected WorkflowExecutor(boolean registerForRMI, String name)  //throws RemoteException
	{
//		this.uri = "//" + Utils.getProp("local_hostname") + "/" + name;
		this.uri = Utils.getProp("local_hostname")+":"+Utils.getIntProp("local_port");
		DBRecord.prepareConnection();
		if (registerForRMI)
		{
//			try
//			{
//				System.out.println("Binding workflow executor to URI: " + uri);
//				Naming.rebind(name, this);
//			}
//			catch (MalformedURLException e)
//			{
//				throw new RuntimeException(e.getMessage(), e);
//			}
//
			
			Server server = new Server();
			try {
				System.out.println("Registrating implementation");
				callHandler.registerGlobal(WorkflowExecutorInterface.class, this);
				System.out.println("Binding");
				server.addServerListener(new IServerListener() {
					public void clientConnected(Socket socket) {
						System.out.println("Client connected: " + socket.getInetAddress());
						
					}
					public void clientDisconnected(Socket socket) {
						System.out.println("Client disconnected: " + socket.getInetAddress());
					}
				});
				server.bind(Utils.getIntProp("local_port"), callHandler, new GZipFilter());
				System.out.println("Server listening");
			} catch (	LipeRMIException | IOException e) {
				e.printStackTrace();
			}
			
			
			if (Utils.hasProp("manager_host")
					&& !Utils.getProp("manager_host").isEmpty()
					&& Utils.hasProp("manager_port")
					&& !Utils.getProp("manager_port").isEmpty())
			{
//				managerURI = "//" + Utils.getProp("manager_host") 
//						+ "/SiteManager@" + Utils.getProp("manager_port");
				managerURI = Utils.getProp("manager_host")+":"+Utils.getProp("manager_port");
				while (manager == null)
				{
					try
					{
						manager = (WorkflowExecutorInterface) WorkflowExecutor
								.getRemoteExecutor(managerURI);
						manager.registerWorker(uri, totalProcessors);
						manager.greeting("Hello from " + uri);
					}
					catch (NotBoundException ex)
					{
						manager = null;
					}
				}
			}
			System.out.println("Done.");
		}
		Utils.mkdirs(Utils.getProp("working_dir"));
	}

	
	private static GZipFilter gzipfilter = new GZipFilter();
	private static HashMap<String, Client> clients = new HashMap<>();
	private static Client getRMIClient(final String uri)
	{
		Client c = clients.get(uri);
		if(c == null)
		{
			String[] s = uri.split(":");
			try
			{
				c = new Client(s[0], Integer.parseInt(s[1]), new CallHandler(), gzipfilter);
				clients.put(uri, c);
			}
			catch (IOException ex)
			{
				throw new RuntimeException(ex.getMessage());
			}
			c.addClientListener(new IClientListener() {
				@Override
				public void disconnected()
				{
					clients.remove(uri);
				}
			});
		}
		
		return c;
	}
	public static WorkflowExecutorInterface getRemoteExecutor(String weURI) throws NotBoundException
	{
//		try
//		{
//			return (WorkflowExecutorInterface) Naming.lookup(weURI);
////			String[] s = weURI.replace("//", "").split("/");
////			return (WorkflowExecutorInterface) LocateRegistry.getRegistry(s[0]).lookup(s[1]);
//		}
//		catch (MalformedURLException|RemoteException e)
//		{
//			throw new RuntimeException(e.getMessage(), e);
//		}
		Client c = getRMIClient(weURI);
		return (WorkflowExecutorInterface) c.getGlobal(WorkflowExecutorInterface.class);
	}
	public static WorkflowExecutorInterface getSiteManager() throws NotBoundException
	{
//		return getRemoteExecutor("//"+Utils.getProp("manager_host")+"/SiteManager@"+Utils.getProp("manager_port"));
		return getRemoteExecutor(Utils.getProp("manager_host")+":"+Utils.getIntProp("manager_port"));
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
