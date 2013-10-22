/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import difsys.DifsysFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import lipermi.net.Client;
import net.fusejna.FuseException;
import workflowengine.schedule.Schedule;
import workflowengine.server.SiteManager;
import workflowengine.server.WorkflowExecutor;
import workflowengine.server.WorkflowExecutorInterface;
import workflowengine.utils.Threading;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class DIFileManager extends FileManager implements DIFileManagerInterface
{

	private static int MAX_UPLOAD_THREADS = 10;
	private static int portShift = 100;
	private HashMap<String, Object> locks = new HashMap<>();
	private static DIFileManager instant;
	private int PIECE_LEN = Utils.getIntProp("fs_piece_size");
	private WorkflowExecutorInterface manager;
	private WorkflowExecutor thisSite;
	private String thisURI = WorkflowExecutor.get().getURI();
//	private final HashMap<String, Thread> uploadThreads = new HashMap<>();
	private final HashSet<Thread> uploadThreads = new HashSet<>();
	private final Object PIECE_RETRIEVED_LOCK = new Object();
	private final Object QUEUE_POLL_LOCK = new Object();
	private DB db;
	private DBCollection reqPcsColl;
	private DBCollection fPrtColl;
	private DBCollection exisPcsColl;
//	private DBCollection tferQColl;
	private DBCollection uinfRetPcsColl;
	private DBCollection readyFileColl;
	private DBCollection inactiveFileColl;
	private DBCollection tferLogColl;
	private int runningUploadThreads = 0;
	private final Object RUNNING_UPLOAD_THREADS_LOCK = new Object();
	private final Queue<String> uploadTargetQueue = new ConcurrentLinkedQueue<>();
	private Set<String> uploadTarget = new HashSet<>();
	private Set<String> peers = new HashSet<>();
	private TransferQueue queue = new TransferQueue();
	//TODO: create index on mongodb

	protected DIFileManager() throws FuseException
	{
		Utils.registerRMIServer(DIFileManagerInterface.class, this, portShift + Utils.getIntProp("local_port"));
		//Init MongoDB
		try
		{
			db = new Mongo(
					Utils.getProp("fs_db_host"),
					Utils.getIntProp("fs_db_port"))
					.getDB(Utils.getProp("fm_db_name") + "_" + thisURI.replace('.', '_').replace(':', '_'));
			db.dropDatabase();
			db = new Mongo(
					Utils.getProp("fs_db_host"),
					Utils.getIntProp("fs_db_port"))
					.getDB(Utils.getProp("fm_db_name") + "_" + thisURI.replace('.', '_').replace(':', '_'));
			reqPcsColl = db.getCollection("required_pieces");
			fPrtColl = db.getCollection("file_priority");
			exisPcsColl = db.getCollection("existing_pieces");
//			tferQColl = db.getCollection("transfer_queue");
			tferLogColl = db.getCollection("transfer_log");
			uinfRetPcsColl = db.getCollection("uninformed_retrived_pieces");
			readyFileColl = db.getCollection("ready_files");
			inactiveFileColl = db.getCollection("inactive_files");

		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
		}


		//Get site manager
		manager = WorkflowExecutor.getSiteManager();
		thisSite = WorkflowExecutor.get();
		if (manager != null)
		{
			peers.add(Utils.getProp("manager_host") + ":" + Utils.getProp("manager_port"));
		}


		Utils.getPROP().put("fs_db_name",
				Utils.getProp("fs_db_name")
				+ "_" + Utils.getProp("local_hostname")
				+ "_" + Utils.getProp("local_port"));

		new difsys.Difsys(Utils.getProp("working_dir"), Utils.getPROP());
		Utils.bash("rm -rf " + Utils.getProp("working_dir") + "/*", false);
		Utils.bash("rm -rf " + Utils.getProp("fs_storage_dir") + "/*", false);
		startListeningThread();
		startUploadThread();
	}

	private void startUploadThread()
	{
		if (manager != null)
		{
			for (final String uri : peers)
			{
				addWorker(uri);
			}
		}
		if (thisSite instanceof SiteManager)
		{
			Set<String> workers = thisSite.getWorkerSet();
			if (workers != null)
			{
				for (final String uri : workers)
				{
					addWorker(uri);
				}
			}
		}
	}

	@Override
	public void addWorker(final String uri)
	{
		if (!uploadTarget.contains(uri))
		{
			queue.addPeer(uri);
			Future f1 = Threading.submitTask(new Runnable()
			{
				@Override
				public void run()
				{
					synchronized (uploadTargetQueue)
					{
						uploadTargetQueue.add(uri);
						uploadTargetQueue.notifyAll();
					}
				}
			});
			Future f2 = Threading.submitTask(new Runnable()
			{
				@Override
				public void run()
				{
					synchronized (RUNNING_UPLOAD_THREADS_LOCK)
					{
						while (runningUploadThreads < MAX_UPLOAD_THREADS)
						{
							Thread t = new Thread("Upload thread for " + uri)
							{
								@Override
								public void run()
								{
									upload();
								}
							};
							t.start();
							uploadThreads.add(t);
							runningUploadThreads++;
						}
					}
				}
			});

			uploadTarget.add(uri);
			try
			{
				f1.get();
				f2.get();
			}
			catch (InterruptedException | ExecutionException ex)
			{
				Logger.getLogger(DIFileManager.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
	}

	/**
	 * Called by site manager only
	 *
	 * @param uri
	 */
	public void workerJoined(String uri)
	{
		addWorker(uri);
		if (thisSite instanceof SiteManager)
		{
			Set<String> workers = thisSite.getWorkerSet();
			if (workers != null)
			{
				workers.remove(uri);
				for (final String w : workers)
				{
					getRemoteFileManager(w).addWorker(uri);
				}
			}
		}
	}


	/**
	 * Get a target peer from queue and start uploading
	 */
	private void upload()
	{
		while (exisPcsColl.count(new BasicDBObject("worker", thisURI)) == 0)
		{
			synchronized (Thread.currentThread())
			{
				try
				{
					Thread.currentThread().wait();
				}
				catch (InterruptedException ex)
				{}
			}
		}
		synchronized (uploadTargetQueue)
		{
			while (uploadTargetQueue.isEmpty())
			{
				try
				{
					uploadTargetQueue.wait();
				}
				catch (InterruptedException ex)
				{
				}
			}
		}
		while (true)
		{
			String target = null;
			synchronized (uploadTargetQueue)
			{
				target = uploadTargetQueue.poll();
				uploadTargetQueue.add(target);
			}
			uploadPiece(target);
		}
	}

	private void uploadPiece(final String toURI)
	{

		//Update retrieved pieces
//		List<DBObject> retrieved = getRemoteFileManager(toURI).getRetrievedPieces(thisURI);
//
//		for (DBObject obj : retrieved)
//		{
//			obj.removeField("_id");
//			reqPcsColl.remove(obj);
////				addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
//			queue.remove(toURI, (String) obj.get("name"), (int) obj.get("no"));
//			obj.put("worker", toURI);
//			exisPcsColl.insert(obj);
//		}

		final PieceInfo p = queue.poll(toURI);
		if (p == null)
		{
			return;
		}

		//Transfer file to toURI
		try
		{
			tferLogColl.insert(new BasicDBObject("time", Utils.time())
					.append("name", p.name)
					.append("no", p.pieceNo)
					.append("worker", toURI)
					.append("type", "send"));
			System.out.print("Uploading "+p.name+p.pieceNo+":"+toURI+"..");
			String[] host = toURI.split(":");
			Socket s = new Socket(host[0], 2 * portShift + Integer.parseInt(host[1]));
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject(thisURI);
			oos.writeObject(p);
			oos.flush();
			if (s.getInputStream().read() == 1)
			{
				byte[] content;
				int len;

				DifsysFile file = DifsysFile.get("/" + p.name, false, false);
				if (file != null)
				{
					content = file.getPieceContent(p.pieceNo).getContent();
					int totalPcs = (int) Math.ceil(p.fileLength / (double) PIECE_LEN);
					if (p.pieceNo < totalPcs - 1)
					{
						len = PIECE_LEN;
					}
					else
					{
						len = (int) (p.fileLength % PIECE_LEN);
					}
				}
				else
				{
					String fname = Utils.getProp("fs_storage_dir") + "/" + p.name + "." + p.pieceNo;
					content = new byte[PIECE_LEN];
					FileInputStream fr = new FileInputStream(fname);
					len = fr.read(content);
					fr.close();
				}
				System.out.println("Done.");
				s.getOutputStream().write(content, 0, len);
			}
			s.close();
			
			Threading.submitTask(new Runnable(){
				@Override
				public void run()
				{
					BasicDBObject obj = new BasicDBObject()
						.append("name", p.name)
						.append("no", p.pieceNo)
						.append("worker", toURI);
					reqPcsColl.remove(obj);
					exisPcsColl.insert(obj);
				}
			});
		}
		catch (IOException | NumberFormatException e)
		{
			thisSite.logger.log("Cannot send filepiece to " + toURI + ": " + e.getMessage(), e);
		}
		System.gc();

	}

	protected void pieceRetrieved(PieceInfo p, String fromWorker)
	{
		tferLogColl.insert(new BasicDBObject("time", Utils.time())
				.append("name", p.name)
				.append("no", p.pieceNo)
				.append("worker", fromWorker)
				.append("type", "receive"));
		reqPcsColl.remove(new BasicDBObject()
				.append("name", p.name)
				.append("no", p.pieceNo));

		BasicDBObject existingPcsObj = new BasicDBObject()
				.append("name", p.name)
				.append("no", p.pieceNo)
				.append("full_file_length", p.fileLength)
				.append("worker", thisURI);
		exisPcsColl.insert(existingPcsObj);
		existingPcsObj.put("worker", fromWorker);
		exisPcsColl.insert(existingPcsObj);

		notifyAllUploadThreads();
		if (p.fileLength < 1)
		{
			System.out.println("Warning: Piece length less than 1.");
			checkIfAllPieceReceived(p.name);
		}
		else
		{
			checkIfAllPieceReceived(p.name, p.fileLength);
		}

		synchronized (PIECE_RETRIEVED_LOCK)
		{
			for (String uri : peers)
			{
				uinfRetPcsColl.insert(new BasicDBObject()
						.append("name", p.name)
						.append("no", p.pieceNo)
						.append("worker", uri));
			}
		}
	}

	private void notifyAllUploadThreads()
	{
		for (final Thread t : uploadThreads)
		{
			synchronized (t)
			{
				t.notifyAll();
			}
		}
	}

	public void checkIfAllPieceReceived(String name)
	{
		DBObject obj = exisPcsColl.findOne(new BasicDBObject()
				.append("name", name)
				.append("worker", thisURI));
		if (obj != null)
		{
			checkIfAllPieceReceived(name, (long) obj.get("full_file_length"));
		}
	}

	public void checkIfAllPieceReceived(String name, long len)
	{
		if (len < 1)
		{
			throw new IllegalArgumentException("Length is less than 1");
		}
		boolean ready = true;
		List<DBObject> exisPcs = exisPcsColl.find(new BasicDBObject()
					.append("name", name)
					.append("worker", thisURI))
				.sort(new BasicDBObject("no", -1)).toArray();
		for (int i = 0; i < Math.ceil(len / (double) PIECE_LEN); i++)
		{
			boolean foundI = false;
			int j;
			for(j=0;j<exisPcs.size();j++)
			{
				if ((int)exisPcs.get(j).get("no") == i)
				{
					foundI = true;
					break;
				}
			}
			if(!foundI)
			{
				ready = false;
				break;
			}
			else
			{
				exisPcs.remove(exisPcs.get(j));
			}
		}

		if (ready)
		{
			readyFileColl.insert(new BasicDBObject("name", name));
			Utils.mkdirs(Utils.getParentPath(Utils.getProp("working_dir") + "/" + name));
			DifsysFile.addFile("/" + name, len);
			Object o = locks.get(name);
			if (o != null)
			{
				synchronized (o)
				{
					o.notifyAll();
				}
			}
		}
	}

	@Override
	public void setPiecesInfo(final String invoker, final List<DBObject> requiredPieces, final List<DBObject> filePriorities)
	{
		if (!invoker.equals(thisURI))
		{
			reqPcsColl.insert(requiredPieces);
		}

		Threading.submitTask(new Runnable()
		{
			@Override
			public void run()
			{
				for (DBObject obj : filePriorities)
				{
					obj.removeField("_id");
					fPrtColl.update(new BasicDBObject("name", obj.get("name")), obj, true, false);
				}
				queue.updateQueuePriority(filePriorities);

				for (DBObject obj : requiredPieces)
				{
					queue.add((String) obj.get("name"),
							(int) obj.get("no"),
							(String) obj.get("worker"));
				}
			}
		});

	}

	/**
	 *
	 * @param invoker
	 * @return list of DBObject containing keys: name, no
	 */
	@Override
	public List<DBObject> getRetrievedPieces(String invoker)
	{
		BasicDBObject invokerQuery = new BasicDBObject("worker", invoker);
		List<DBObject> list = uinfRetPcsColl.find(invokerQuery).toArray();
		synchronized (PIECE_RETRIEVED_LOCK)
		{
			uinfRetPcsColl.remove(invokerQuery);
		}
		return list;
	}

	/**
	 * Record that which files are required by which sites. Also record that all
	 * input file of workflow is existing
	 *
	 * @param s
	 */
	public void setSchedule(Schedule s)
	{
		Workflow wf = s.getSettings().getWorkflow();
		String wfid = wf.getSuperWfid();

		//Set file requirements of input files of all tasks
		for (String tid : wf.getTaskSet())
		{
			Task t = Task.get(tid);
			String worker = s.getWorkerForTask(tid);


			for (String fid : t.getInputFiles())
			{
				WorkflowFile file = WorkflowFile.get(fid);
				reqPcsColl.insert(new BasicDBObject()
						.append("worker", worker)
						.append("name", file.getName(wfid))
						.append("no", -1));
				fPrtColl.update(
						new BasicDBObject("name", file.getName(wfid)),
						new BasicDBObject()
						.append("name", file.getName(wfid))
						.append("priority", file.getPriority()), true, false);
			}
		}

		for (String fid : wf.getOutputFiles())
		{
			WorkflowFile file = WorkflowFile.get(fid);
			reqPcsColl.insert(new BasicDBObject()
					.append("worker", thisURI)
					.append("name", file.getName(wfid))
					.append("no", -1));
			fPrtColl.update(
					new BasicDBObject("name", file.getName(wfid)),
					new BasicDBObject()
					.append("name", file.getName(wfid))
					.append("priority", file.getPriority()), true, false);
		}

		//Set workflow input files as existing files
		for (String fid : wf.getInputFiles())
		{
			WorkflowFile file = WorkflowFile.get(fid);
			fPrtColl.update(
					new BasicDBObject("name", file.getName(wfid)),
					new BasicDBObject()
					.append("name", file.getName(wfid))
					.append("priority", file.getPriority()), true, false);
			for (int i = 0; i < Math.ceil(file.getSize() / (double) PIECE_LEN); i++)
			{
				exisPcsColl.insert(new BasicDBObject()
						.append("name", file.getName(wfid))
						.append("no", i)
						.append("full_file_length", (long) file.getSize())
						.append("worker", thisURI));
			}
			readyFileColl.insert(new BasicDBObject("name", file.getName(wfid)));
		}


		List<DBObject> reqPcs = reqPcsColl.find().toArray();
		List<DBObject> fPrt = fPrtColl.find().toArray();
		setPiecesInfo(thisURI, reqPcs, fPrt);
		for (String uri : thisSite.getWorkerSet())
		{
			DIFileManagerInterface fm = getRemoteFileManager(uri);
//			fm.setFilePriority(fPrt);
			fm.setPiecesInfo(thisURI, reqPcs, fPrt);
		}

		notifyAllUploadThreads();
	}

	private void startListeningThread()
	{
		try
		{
			int port = portShift * 2 + Utils.getIntProp("local_port");
			System.out.println("Listening for file pieces on " + port);
			final ServerSocket ss = new ServerSocket(port);
			new Thread()
			{
				@Override
				public void run()
				{
					while (true)
					{
						try
						{
							Socket s = ss.accept();
							uploadRequestAccepted(s);
						}
						catch (IOException ex)
						{
							Logger.getLogger(DIFileManager.class.getName()).log(Level.SEVERE, null, ex);
						}
					}
				}
			}.start();
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFileManager.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	protected void uploadRequestAccepted(final Socket s)
	{
		Threading.submitTask(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					InputStream in = s.getInputStream();
					ObjectInputStream oos = new ObjectInputStream(in);
					String fromURI = (String) oos.readObject();
					PieceInfo p = (PieceInfo) oos.readObject();
					String fname = Utils.getProp("fs_storage_dir") + "/" + p.name + "." + p.pieceNo;

					OutputStream os = s.getOutputStream();

					boolean pieceExists = exisPcsColl.count(new BasicDBObject()
							.append("name", p.name)
							.append("no", p.pieceNo)
							.append("worker", thisURI)) > 0;
//					if (existingPieces.contains(p))
					if (pieceExists)
					{
						os.write(0);
						os.flush();
					}
					else
					{
						os.write(1);
						os.flush();

//						System.out.println("Receiving " + fname + "...");

						//Create dir in fs_storage
						Utils.mkdirs(Utils.getParentPath(fname));

						//Create logical dir in working_dir
						DifsysFile.get(Utils.getParentPath(
								"/" + p.name + "." + p.pieceNo), true, true);

						FileOutputStream fos = new FileOutputStream(fname);
						ReadableByteChannel inch = Channels.newChannel(in);
						FileChannel fch = fos.getChannel();
						long offset = 0;
						long count;
						while ((count = fch.transferFrom(inch, offset, PIECE_LEN)) > 0)
						{
							offset += count;
						}
						fos.close();
						pieceRetrieved(p, fromURI);
//						System.out.println("Done.");
					}

					s.close();
				}
				catch (IOException | ClassNotFoundException ex)
				{
					Logger.getLogger(DIFileManager.class.getName()).log(Level.SEVERE, null, ex);
				}
				System.gc();
			}
		});
	}

	public static DIFileManagerInterface getRemoteFileManager(String weURI)
	{
		DIFileManagerInterface fm = null;
		int tries = 0;
		String[] s = weURI.split(":");
		String uri = s[0] + ":" + (portShift + Integer.parseInt(s[1]));
		while (fm == null && tries < 10)
		{
			try
			{
				Client c = Utils.getRMIClient(uri);
				fm = (DIFileManagerInterface) c.getGlobal(DIFileManagerInterface.class);
			}
			catch (Exception e)
			{
				fm = null;
				tries++;
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException ex)
				{
				}
			}
		}
		return fm;
	}

	public static DIFileManager get()
	{
		if (instant == null)
		{
			try
			{
				instant = new DIFileManager();
			}
			catch (FuseException ex)
			{
			}
		}
		return instant;
	}

	public void waitForFile(String name)
	{
//		if(Utils.fileExists(Utils.getProp("working_dir")+"/"+fname) || readyFiles.contains(fname))
		if (Utils.fileExists(Utils.getProp("working_dir") + "/" + name)
				|| readyFileColl.count(new BasicDBObject("name", name)) > 0)
		{
			return;
		}
		locks.put(name, name);
		synchronized (name)
		{
//			while (!readyFiles.contains(fname))
			while (!Utils.fileExists(Utils.getProp("working_dir") + "/" + name)
					&& readyFileColl.count(new BasicDBObject("name", name)) == 0)
			{
				try
				{
					name.wait(5000);
				}
				catch (InterruptedException ex)
				{
				}
				checkIfAllPieceReceived(name);
			}
		}
		locks.remove(name);
	}

	@Override
	public void outputFilesCreated(Set<String> filenames)
	{
		for (String fname : filenames)
		{
			long size = new File(Utils.getProp("working_dir") + "/" + fname).length();
			for (int i = 0; i < Math.ceil(size / (double) PIECE_LEN); i++)
			{
				exisPcsColl.insert(new BasicDBObject().append("name", fname).append("no", i)
						.append("worker", thisURI)
						.append("full_file_length", size));
			}
			readyFileColl.insert(new BasicDBObject().append("name", fname));
			queue.add(fname);
		}
		notifyAllUploadThreads();
	}

	/**
	 * Wait until the specified file exists. Download file from manager if not
	 * exists
	 *
	 * @param filename
	 */
//	public void waitForFile(WorkflowFile wff, String workflowDirName)
//	{
//		String fullpath = Utils.getProp("working_dir") + "/" + workflowDirName + "/" + wff.getName();
//		if (!Utils.fileExists(fullpath))
//		{
//			String remoteWorkingDir = WorkflowExecutor.getSiteManager().getWorkingDir();
//			SFTPClient.get(Utils.getProp("manager_host"),
//					remoteWorkingDir + "/" + workflowDirName + "/" + wff.getName(),
//					Utils.getProp("working_dir") + "/" + workflowDirName);
//			if (wff.getType() == WorkflowFile.TYPE_EXEC)
//			{
//				Utils.setExecutable(fullpath);
//			}
//		}
//	}
	/**
	 * Report the manager that the output file is created and ready to be
	 * transferred
	 *
	 * @param wff
	 */
//	public void outputCreated(WorkflowFile wff, String workflowDirName)
//	{
//		String fullpath = Utils.getProp("working_dir") + "/" + workflowDirName + "/" + wff.getName();
//		String remoteWorkingDir = WorkflowExecutor.getSiteManager().getWorkingDir();
//		SFTPClient.put(Utils.getProp("manager_host"),
//				fullpath,
//				remoteWorkingDir + "/" + workflowDirName);
//		
//	}
	public void setPeerSet(String uri, Set<String> peers)
	{
		getRemoteFileManager(uri).setPeerSet(peers);
	}

	@Override
	public void setPeerSet(Set<String> workers)
	{
		this.peers = new HashSet<>(workers);
		this.peers.remove(thisURI);
		for (String p : peers)
		{
			addWorker(p);
		}
	}

	@Override
	public void setInactiveFile(Set<String> files)
	{
		for (String n : files)
		{
			inactiveFileColl.insert(new BasicDBObject("name", n));
		}
		if (manager instanceof SiteManager)
		{
			for (String worker : manager.getWorkerSet())
			{
				getRemoteFileManager(worker).setInactiveFile(files);
			}
		}
	}

	public String getSentPieceHTML()
	{
		StringBuilder sb = new StringBuilder();
		DBCursor c = tferLogColl.find(new BasicDBObject("type", "send"));
		while(c.hasNext())
		{
			DBObject o = c.next();
			sb.append("<div>")
					.append(o.get("time").toString())
					.append(":")
					.append(o.get("name").toString())
					.append(o.get("no").toString())
					.append(" - ")
					.append(o.get("worker").toString())
					.append("</div>");
		}
		return sb.toString();
	}
	public String getReceivePieceHTML()
	{
		StringBuilder sb = new StringBuilder();
		DBCursor c = tferLogColl.find(new BasicDBObject("type", "receive"));
		while(c.hasNext())
		{
			DBObject o = c.next();
			sb.append("<div>")
					.append(o.get("time").toString())
					.append(":")
					.append(o.get("name").toString())
					.append(o.get("no").toString())
					.append(" - ")
					.append(o.get("worker").toString())
					.append("</div>");
		}
		return sb.toString();
	}
	
	
	
	
	
	
	
	
	
	private class TransferQueue
	{
		private HashMap<String, Object> transferLocks = new HashMap<>();
		private final Object ADD_LOCK = new Object();
		
		private HashMap<String, DBCollection> collections = new HashMap<>();
		
		private DBCollection getColl(String target)
		{
			DBCollection c = collections.get(target);
			if(c == null)
			{
				c = db.getCollection("transfer_queue_"+target.replace('.', '_').replace(':', '_'));
				collections.put(target, c);
			}
			return c;
		}
		
		public void addPeer(String uri)
		{
			if (!transferLocks.containsKey(uri))
			{
				transferLocks.put(uri, new Object());
			}
		}

		private void fillQueue(String target)
		{
			DBCollection tferQColl = getColl(target);
			final Object LOCK = transferLocks.get(target);
			synchronized (LOCK)
			{
				if (tferQColl.count() == 0)
				{
					addRequiredPiecesToQueue(target);
					if (tferQColl.count() == 0)
					{
						DBCursor cursor = exisPcsColl.find(new BasicDBObject("worker", thisURI));
						while (cursor.hasNext())
						{
							DBObject obj = cursor.next();
							add((String) obj.get("name"), (int) obj.get("no"), target);
						}
					}
				}
			}
		}
		
		public PieceInfo poll(final String target)
		{
			DBCollection tferQColl = getColl(target);
			final Object LOCK = transferLocks.get(target);
			BasicDBObject prioritySortQuery = new BasicDBObject("priority", -1);

			DBObject nextQueueItem = null;
			synchronized (LOCK)
			{
				while (nextQueueItem == null && tferQColl.count() > 0)
				{
					try
					{
						nextQueueItem = tferQColl.find()
								.sort(prioritySortQuery)
								.limit(1).next();
					}
					catch(RuntimeException e)
					{
						nextQueueItem = null;
						break;
					}
					tferQColl.remove(nextQueueItem);
					boolean alreadySend = tferLogColl.count(new BasicDBObject("type", "send")
							.append("name", nextQueueItem.get("name"))
							.append("no", nextQueueItem.get("no"))
							.append("worker", target)) > 0;
					if(alreadySend)
					{
						nextQueueItem = null;
					}
				}
			}
			PieceInfo p = null;
			if(nextQueueItem != null)
			{
				p = PieceInfo.get((String) nextQueueItem.get("name"),
						(int) nextQueueItem.get("no"), -1.0);
				p.fileLength = (long) nextQueueItem.get("full_file_length");				
			}
			else
			{
				Threading.submitTask(new Runnable(){
					@Override
					public void run()
					{
						fillQueue(target);
					}
				});
			}
			
			return p;
		}

		public void remove(String target, String name, int no)
		{
			DBCollection tferQColl = getColl(target);
			final Object LOCK = transferLocks.get(target);
			synchronized (LOCK)
			{
				tferQColl.remove(new BasicDBObject()
					.append("name", name)
					.append("no", no));
			}
		}

		private void add(String name)
		{
			for (String target : peers)
			{
				add(name, -1, target);
			}
		}

		/**
		 * Add filepiece to transfer queue. If filepiece is already in the
		 * queue, the priority is updated
		 *
		 * @param reqPiece must have field name, no
		 */
		private void add(String name, int no, String targetWorker)
		{
			DBCollection tferQColl = getColl(targetWorker);
			synchronized(ADD_LOCK)
			{
				if (inactiveFileColl.count(new BasicDBObject("name", name)) > 0)
				{
					return;
				}
				BasicDBObject extPcsQuery = new BasicDBObject("name", name).append("worker", thisURI);

				if (no != -1)
				{
					BasicDBObject xferEntry = new BasicDBObject("name", name)
							.append("no", no);
					boolean transferExists = tferQColl.count(xferEntry) > 0 
							|| exisPcsColl.count(xferEntry) > 0;
					if (transferExists)
					{
						return;
					}
					extPcsQuery.append("no", no);
				}
				boolean pieceExists = exisPcsColl.count(extPcsQuery) > 0;

				if (!pieceExists)
				{
					return;
				}

				double priority = 1;
				DBObject priorityObj = fPrtColl.findOne(new BasicDBObject("name", name));
				if (priorityObj != null)
				{
					priority = (double) priorityObj.get("priority");
				}

				if (no == -1)
				{
					DBCursor cursor = exisPcsColl.find(extPcsQuery);
					while (cursor.hasNext())
					{
						DBObject obj = cursor.next();
						no = (int) obj.get("no");
						boolean transferExists = tferQColl.count(new BasicDBObject("name", name)
								.append("no", no)) > 0 
								|| exisPcsColl.count(new BasicDBObject("name", name)
								.append("no", no).append("worker", targetWorker)) > 0;

						if (!transferExists)
						{
							addToQueueNoCheck(targetWorker, name, no, priority);
						}
					}
				}
				else
				{
					addToQueueNoCheck(targetWorker, name, no, priority);
				}
			}
			System.gc();
		}

		private void addToQueueNoCheck(String targetWorker, String name, int no, double priority)
		{
			DBCollection tferQColl = getColl(targetWorker);
			boolean targetNeeded = reqPcsColl.count(new BasicDBObject("name", name)
					.append("worker", targetWorker)
					.append("no", no)) > 0;
			int reqSites = (int) reqPcsColl.count(
					new BasicDBObject("name", name)
					.append("no", no)
					);
			DBObject obj = exisPcsColl.findOne(
					new BasicDBObject("name", name)
					.append("worker", thisURI).append("no", no)
					);
			tferQColl.update(
					new BasicDBObject()
					.append("name", name)
					.append("no", no),
					new BasicDBObject()
					.append("name", name)
					.append("no", no)
					.append("priority", calTransferPriority(priority, targetNeeded, reqSites))
					.append("full_file_length", obj.get("full_file_length")),
					true, false);
		}
		
		private void updateQueuePriority(List<DBObject> priorities)
		{
			for (DBObject obj : priorities)
			{
				for (DBCollection tferQColl : collections.values())
				{
					String name = (String) obj.get("name");
					double priority = (double) obj.get("priority");
					BasicDBObject nameQuery = new BasicDBObject("name", name);
					DBCursor c = tferQColl.find(nameQuery);
					while (c.hasNext())
					{
						DBObject o = c.next();
						//addToQueue((String)o.get("name"), (int)o.get("no"), (String)o.get("worker"));
						String targetWorker = (String) o.get("worker");
						int no = (int) o.get("no");
						nameQuery.append("no", no);
						boolean targetNeeded = reqPcsColl.count(new BasicDBObject("name", name)
								.append("worker", targetWorker)
								.append("no", no)) > 0;
						int reqSites = (int) reqPcsColl.count(nameQuery);

						tferQColl.update(
								new BasicDBObject()
								.append("name", name)
								.append("no", no),
								new BasicDBObject()
								.append("$set",
								new BasicDBObject("priority",
								calTransferPriority(priority, targetNeeded, reqSites))));
					}
				}
			}
		}

		private double calTransferPriority(double filePriority, boolean isRequired, int requiringSites)
		{
			return filePriority * (requiringSites + 1) + 1 + ((isRequired ? 0 : 1) * filePriority);
		}
		
		
		private void addRequiredPiecesToQueue(String toURI)
		{
			DBCursor cursor = reqPcsColl.find(new BasicDBObject("worker", toURI));
			while (cursor.hasNext())
			{
				DBObject obj = cursor.next();
				queue.add((String) obj.get("name"), (int) obj.get("no"), toURI);
			}
		}
	}
}
