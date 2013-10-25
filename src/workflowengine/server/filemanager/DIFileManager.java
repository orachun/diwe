/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import com.mongodb.BasicDBList;
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
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
	private static int MAX_UPLOAD_THREADS = 2;
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
	private DB db;
	private DBCollection reqPcsColl;
	private DBCollection fPrtColl;
	private DBCollection exisPcsColl;
	private DBCollection notAddedExisPcsColl;
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
	private static ExecutorService downloadThreadPool = Executors.newFixedThreadPool(2);

	private static boolean shuttingDown = false;
	
	private Thread pieceListeningThread;
	
	private boolean uploadThreadSleeping = false;
	
	
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
			notAddedExisPcsColl = db.getCollection("not_added_existing_pieces");
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

		initDifsys();

		Utils.bash("rm -rf " + Utils.getProp("working_dir") + "/*", false);
		Utils.bash("rm -rf " + Utils.getProp("fs_storage_dir") + "/*", false);
		startListeningThread();
		startUploadThread();
	}

	public static void initDifsys() throws FuseException
	{
		Utils.getPROP().put("fs_db_name",
				Utils.getProp("fs_db_name")
				+ "_" + Utils.getProp("local_hostname")
				+ "_" + Utils.getProp("local_port"));

		new difsys.Difsys(Utils.getProp("working_dir"), Utils.getPROP());
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
	public void shutdown()
	{
		shuttingDown = true;
		downloadThreadPool.shutdownNow();
		
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

	private Integer noPieceCount = 0;
	/**
	 * Get a target peer from queue and start uploading
	 */
	private void upload()
	{
		while (!uploadThreadSleeping && exisPcsColl.count(new BasicDBObject("worker", thisURI)) == 0)
		{
			synchronized (Thread.currentThread())
			{
				try
				{
					Thread.currentThread().wait(5000);
				}
				catch (InterruptedException ex)
				{
				}
			}
		}
		uploadThreadSleeping = true;
		while (!shuttingDown)
		{
			String target;
			synchronized (uploadTargetQueue)
			{
				target = uploadTargetQueue.poll();
				uploadTargetQueue.add(target);
			}
			
			if(!uploadPiece(target))
			{
				noPieceCount++;
				if(noPieceCount >= uploadTargetQueue.size())
				{
					synchronized (Thread.currentThread())
					{
						try
						{
							Thread.currentThread().wait(5000);
						}
						catch (InterruptedException ex)
						{
						}
					}
					noPieceCount = 0;
				}
			}
		}
	}

	private boolean uploadPiece(final String toURI)
	{
		final PieceInfo p = queue.poll(toURI);
		if (p == null)
		{
			return false;
		}
		
		//Update retrieved pieces
		if(Math.random() > 0.8)
		{
			List<DBObject> retrieved = getRemoteFileManager(toURI).getRetrievedPieces(thisURI);

			for (DBObject obj : retrieved)
			{
				obj.removeField("_id");
				reqPcsColl.remove(new BasicDBObject("name", obj.get("name"))
						.append("no", obj.get("no"))
						.append("worker", toURI));
	////				addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
				queue.remove(toURI, (String) obj.get("name"), (int) obj.get("no"));
				queue.pieceRetrieved((String) obj.get("name"), (int) obj.get("no"), toURI, -1);
	//			obj.put("worker", toURI);
	//			exisPcsColl.insert(obj);
			}
		}


		//Transfer file to toURI
		try
		{
			tferLogColl.insert(new BasicDBObject("time", Utils.time())
					.append("name", p.name)
					.append("no", p.pieceNo)
					.append("worker", toURI)
					.append("type", "send"));
//			System.out.println("Upload " + p.name + p.pieceNo + ":" + toURI);
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
				
//				System.out.println("Done.");
				s.getOutputStream().write(content, 0, len);
				s.shutdownOutput();
				s.getInputStream().read();
			}
			s.close();

			Threading.submitTask(new Runnable()
			{
				@Override
				public void run()
				{
					BasicDBObject obj = new BasicDBObject()
							.append("name", p.name)
							.append("no", p.pieceNo)
							.append("worker", toURI);
					reqPcsColl.remove(obj);
					
					queue.pieceRetrieved(p.name, p.pieceNo, toURI, p.fileLength);
				}
			});
		}
		catch (IOException | NumberFormatException e)
		{
			thisSite.logger.log("Cannot send filepiece to " + toURI + ": " + e.getMessage(), e);
		}
		System.gc();
		return true;
	}

	protected void pieceRetrieved(PieceInfo p, String fromWorker)
	{
		tferLogColl.insert(new BasicDBObject("time", Utils.time())
				.append("name", p.name)
				.append("no", p.pieceNo)
				.append("worker", fromWorker)
				.append("type", "receive"));
		
		queue.pieceRetrieved(p.name, p.pieceNo, fromWorker, p.fileLength);
		queue.pieceRetrieved(p.name, p.pieceNo, thisURI, p.fileLength);
		
		reqPcsColl.remove(new BasicDBObject()
				.append("name", p.name)
				.append("no", p.pieceNo)
				.append("worker", fromWorker));
		
		reqPcsColl.remove(new BasicDBObject()
				.append("name", p.name)
				.append("no", p.pieceNo)
				.append("worker", thisURI));
		
		queue.add(p.name, p.pieceNo, true, false, true);
		
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
		new Thread()
		{
			@Override
			public void run()
			{
				for (final Thread t : uploadThreads)
				{
					synchronized (t)
					{
						t.notifyAll();
					}
				}
			}
		}.start();
		
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
		int totalPcs = (int)Math.ceil(len / (double) PIECE_LEN);
		List nos = exisPcsColl.distinct("no", new BasicDBObject()
				.append("name", name)
				.append("worker", thisURI));
		boolean ready = nos.size() == totalPcs;

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
//				queue.updateQueuePriority(filePriorities);

//				for (DBObject obj : requiredPieces)
//				{
//					queue.add((String) obj.get("name"),
//							(int) obj.get("no"),
//							(String) obj.get("worker"));
//				}
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

		//Set file priority
		for (String fid : wf.getInputFiles())
		{
			WorkflowFile file = WorkflowFile.get(fid);
			fPrtColl.update(
					new BasicDBObject("name", file.getName(wfid)),
					new BasicDBObject()
					.append("name", file.getName(wfid))
					.append("priority", file.getPriority()), true, false);
		}

		//Set and broadcast piece information
		List<DBObject> reqPcs = reqPcsColl.find().toArray();
		List<DBObject> fPrt = fPrtColl.find().toArray();
		setPiecesInfo(thisURI, reqPcs, fPrt);
		for (String uri : thisSite.getWorkerSet())
		{
			DIFileManagerInterface fm = getRemoteFileManager(uri);
			fm.setPiecesInfo(thisURI, reqPcs, fPrt);
		}

		//Set workflow input files as existing files
		for (String fid : wf.getInputFiles())
		{
			WorkflowFile file = WorkflowFile.get(fid);
			for (int i = 0; i < Math.ceil(file.getSize() / (double) PIECE_LEN); i++)
			{
				exisPcsColl.insert(new BasicDBObject("name", file.getName(wfid))
					.append("no", i)
					.append("worker", thisURI)
					.append("full_file_length", (long) file.getSize())
					.append("_r", Math.random())
					);
			}
			readyFileColl.insert(new BasicDBObject("name", file.getName(wfid)));
		}
		
		//Add existing pieces into transfer queue
		for (DBObject obj : exisPcsColl.find(new BasicDBObject("worker", thisURI)).toArray())
		{
			String name = (String) obj.get("name");
			int no = (int) obj.get("no");
			BasicDBList or = new BasicDBList();
			or.add(new BasicDBObject("no", no));
			or.add(new BasicDBObject("no", -1));
			
			for(String p:peers)
			{
				boolean require = reqPcsColl.count(new BasicDBObject("name", name).append("worker", p)
						.append("$or", or)) > 0;
				if(require)
				{
					//don't check existing, requiring, inactive
					queue.add((String) obj.get("name"), (int) obj.get("no"), p, false, false, false);
				}
			}
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
			pieceListeningThread = new Thread("Piece listening thread")
			{
				@Override
				public void run()
				{
					while (!shuttingDown)
					{
						try
						{
							Socket s = ss.accept();
							uploadRequestAccepted(s);
						}
						catch (IOException ex)
						{}
					}
				}
			};
			pieceListeningThread.start();
		}
		catch (IOException ex)
		{
			thisSite.logger.log("Cannot start piece listening thread.", ex);
		}

	}

	protected void uploadRequestAccepted(final Socket s)
	{
		downloadThreadPool.submit(new Runnable()
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
					String fname = Utils.getProp("fs_storage_dir")
							+ "/" + p.name + "." + p.pieceNo;

					OutputStream os = s.getOutputStream();

					boolean pieceExists = exisPcsColl.count(new BasicDBObject()
							.append("name", p.name)
							.append("no", p.pieceNo)
							.append("worker", thisURI)) > 0;
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
						os.write(1);
						os.flush();
						pieceRetrieved(p, fromURI);
						fos.close();
					}
					s.close();
				}
				catch (IOException | ClassNotFoundException ex)
				{
					thisSite.logger.log("Cannot receive file piece.", ex);
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

	@Override
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
				//checkIfAllPieceReceived(name);
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
				queue.pieceRetrieved(fname, i, thisURI, size);
				queue.add(fname, i, false, false, true);
			}
			readyFileColl.insert(new BasicDBObject().append("name", fname));
		}
		notifyAllUploadThreads();
	}

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
	
	public void addPeer(String uri, String peer)
	{
		getRemoteFileManager(uri).addPeer(peer);
	}
	
	@Override
	public void addPeer(String peer)
	{
		if(!peer.equals(thisURI))
		{
			peers.add(peer);
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
		while (c.hasNext())
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
		while (c.hasNext())
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
		private Set<String> peers = new HashSet<>();
		private final Object ADD_LOCK = new Object();
		private HashMap<String, DBCollection> collections = new HashMap<>();
		private HashMap<String, Queue<PieceInfo>> queueCache = new HashMap<>();
		
		private void fillQueueCache(final String target)
		{
			Queue<PieceInfo> queue = queueCache.get(target);

			while (!shuttingDown)
			{
				DBCollection tferQColl = getColl(target);
				synchronized (queue)
				{
					while (queue.size() > 10)
					{
						try
						{
							queue.wait(10000);
						}
						catch (InterruptedException ex)
						{
							if(shuttingDown)
							{
								break;
							}
						}
					}
				}
				if(shuttingDown)
				{
					break;
				}
				boolean added = false;
				BasicDBObject prioritySortQuery = new BasicDBObject("priority", -1);
				DBObject nextQueueItem = null;
				DBCursor cursor = tferQColl.find()
						.sort(prioritySortQuery)
						.limit(5);
				while (cursor.hasNext())
				{
					try
					{
						nextQueueItem = cursor.next();
					}
					catch (RuntimeException e)
					{
						nextQueueItem = null;
						break;
					}
					tferQColl.remove(nextQueueItem);
					boolean alreadySend = tferLogColl.count(new BasicDBObject("type", "send")
							.append("name", nextQueueItem.get("name"))
							.append("no", nextQueueItem.get("no"))
							.append("worker", target)) > 0;
					if (alreadySend)
					{
						nextQueueItem = null;
					}
					else
					{
						PieceInfo p = PieceInfo.get((String) nextQueueItem.get("name"),
								(int) nextQueueItem.get("no"), -1.0, 
								(long) nextQueueItem.get("full_file_length"));
						queue.add(p);
						added = true;
						
					}
				}
//				if (tferQColl.count() < 5)
//				{
////					fillQueue(target);
//					DBObject obj = exisPcsColl.findOne(new BasicDBObject("worker", thisURI)
//							.append("_r", new BasicDBObject("$gt", Math.random())));
//					if(obj != null)
//					{
//						add((String)obj.get("name"), 
//								(int)obj.get("no"), target);
//					}
//			
//				}
			}
		}

		private DBCollection getColl(String target)
		{
			DBCollection c = collections.get(target);
			if (c == null)
			{
				c = db.getCollection("transfer_queue_" + target.replace('.', '_').replace(':', '_'));
				collections.put(target, c);
			}
			return c;
		}

		public void addPeer(final String uri)
		{
			if (!peers.contains(uri))
			{
				peers.add(uri);
				queueCache.put(uri, new ConcurrentLinkedQueue<PieceInfo>());
				Thread t = new Thread("Queue cache filling")
				{
					@Override
					public void run()
					{
						fillQueueCache(uri);
					}
				};
				t.start();
			}
		}
		
		private void fillQueue(String target)
		{
			DBCollection tferQColl = getColl(target);
//			final Object LOCK = transferLocks.get(target);
//			synchronized (LOCK)
			{
				if (tferQColl.count() == 0)
				{
					//addRequiredPiecesToQueue(target);
					//if (tferQColl.count() == 0)
					{
						DBCursor cursor = notAddedExisPcsColl
								.find(new BasicDBObject("_random", 
									new BasicDBObject("$gt", Math.random()))
									.append("worker", target))
								.sort(new BasicDBObject("_random", 1))
								.limit(100);
//						while (cursor.hasNext())
//						{
//							DBObject obj = cursor.next();
//							add((String) obj.get("name"), (int) obj.get("no"), target);
//						}
						
						List<DBObject> objs = cursor.toArray();
//						Collections.shuffle(objs);
						for(DBObject obj:objs)
						{
							add((String) obj.get("name"), (int) obj.get("no"), target);
						}
						for(DBObject obj:objs)
						{
							notAddedExisPcsColl.remove(obj);
						}
					}
				}
			}
		}

		
		public PieceInfo poll(final String target)
		{
			final Queue<PieceInfo> queue = queueCache.get(target);
			PieceInfo p;
			p = queue.poll();
			
			if(p == null && Math.random() > 0.8)
			{
				double r = Math.random();
				DBObject obj = exisPcsColl.findOne(new BasicDBObject("worker", thisURI)
							.append("_r", new BasicDBObject("$lt", r)),
						new BasicDBObject(),
						new BasicDBObject("_r", -1));
				if(obj == null)
				{
					obj = exisPcsColl.findOne(new BasicDBObject("worker", thisURI)
							.append("_r", new BasicDBObject("$gt", r)),
						new BasicDBObject(),
						new BasicDBObject("_r", 1));
				}
				
				if(obj != null)
				{
					String name = (String)obj.get("name");
					int no = (int)obj.get("no");
					boolean isRequired = 
							reqPcsColl.count(
								new BasicDBObject("name", name)
									.append("no", no)
									.append("worker", new BasicDBObject("$ne", thisURI))
							) > 0 && inactiveFileColl.count(new BasicDBObject("name", name)) == 0;
					if(isRequired)
					{
						p = PieceInfo.get(name, no, -1, (long)obj.get("full_file_length"));
					}
				}
			}
			
			synchronized (queue)
			{
				queue.notifyAll();
			}
			return p;
		}
		

		public void remove(String target, String name, int no)
		{
			DBCollection tferQColl = getColl(target);
//			final Object LOCK = transferLocks.get(target);
//			synchronized (LOCK)
			{
				tferQColl.remove(new BasicDBObject()
						.append("name", name)
						.append("no", no));
			}
		}

		private void add(String name, int no, boolean checkInactive, boolean checkExisting, boolean checkRequired)
		{
			for (String target : peers)
			{
				add(name, no, target, checkInactive, checkExisting, checkRequired);
			}
		}

		private void add(String name, int no, String targetWorker)
		{
			add(name, no, targetWorker, true, true, true);
		}
		
		/**
		 * Add filepiece to transfer queue. If filepiece is already in the
		 * queue, the priority is updated
		 *
		 * @param reqPiece must have field name, no
		 */
		private void add(String name, int no, final String targetWorker, boolean checkInactive, boolean checkExisting, boolean checkRequired)
		{
//			DBCollection tferQColl = getColl(targetWorker);
			synchronized (ADD_LOCK)
			{
				if (checkInactive && inactiveFileColl.count(new BasicDBObject("name", name)) > 0)
				{
					return;
				}
				
				BasicDBObject extPcsQuery = new BasicDBObject("name", name).append("worker", thisURI);
				if (no != -1)
				{
					extPcsQuery.append("no", no);
				}
				
				if (checkExisting && exisPcsColl.count(extPcsQuery) == 0)
				{
					return;
				}
				
				if (no == -1)
				{					
					double priority = 0.1;
					DBObject priorityObj = fPrtColl.findOne(new BasicDBObject("name", name));
					if (priorityObj != null)
					{
						priority = (double) priorityObj.get("priority");
					}
					DBCursor cursor = exisPcsColl.find(extPcsQuery);
					while (cursor.hasNext())
					{
						DBObject obj = cursor.next();
						no = (int) obj.get("no");
						
						
						boolean isRequired = checkRequired ? 
								reqPcsColl.count(
									new BasicDBObject("name", name)
										.append("no", no)
										.append("worker", targetWorker)
									) > 0 
								: true;
						if (isRequired)
						{
							addToQueueNoCheck(targetWorker, name, no, priority);
						}
					}
				}
				else
				{
					BasicDBList or = new BasicDBList();
					or.add(new BasicDBObject("no", no));
					or.add(new BasicDBObject("no", -1));
					boolean isRequired = checkRequired ? 
							reqPcsColl.count(
								new BasicDBObject("name", name)
									.append("worker", targetWorker)
									.append("$or", or)
								) > 0
							: true;
					if (isRequired)
					{
						double priority = 0.1;
						DBObject priorityObj = fPrtColl.findOne(new BasicDBObject("name", name));
						if (priorityObj != null)
						{
							priority = (double) priorityObj.get("priority");
						}
						addToQueueNoCheck(targetWorker, name, no, priority);
					}
				}
			}

			new Thread(){
				@Override
				public void run()
				{
					Queue q = queueCache.get(targetWorker);
					synchronized (q)
					{
						q.notifyAll();
					}
				}
			}.start();
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
					.append("no", no));
			DBObject obj = exisPcsColl.findOne(
					new BasicDBObject("name", name)
					.append("worker", thisURI).append("no", no));
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
			if(filePriority < 0)
			{
				return 0;
			}
			return filePriority * (requiringSites + 1) + 1 + ((isRequired ? 10 : 0) * filePriority) + Math.random();
		}
		
		private double calTransferPriority(String name, int no, String target)
		{
			boolean targetNeeded = reqPcsColl.count(new BasicDBObject("name", name)
					.append("worker", target)
					.append("no", no)) > 0;
			int reqSites = (int) reqPcsColl.count(
					new BasicDBObject("name", name)
					.append("no", no));
			double priority = 1;
			DBObject priorityObj = fPrtColl.findOne(new BasicDBObject("name", name));
			if (priorityObj != null)
			{
				priority = (double) priorityObj.get("priority");
			}
			return calTransferPriority(priority, targetNeeded, reqSites);
		}

		public void pieceRetrieved(String name, int no, String receiver, long fullLength)
		{
			exisPcsColl.insert(new BasicDBObject("name", name)
					.append("no", no)
					.append("worker", receiver)
					.append("full_file_length", fullLength)
					.append("_r", Math.random())
					);
			
			synchronized(this)
			{
				DBCursor cursor = reqPcsColl.find(new BasicDBObject("name", name).append("no", -1));
				boolean remove = false;
				while(cursor.hasNext())
				{
					remove = true;
					DBObject obj = cursor.next();
					for(int i=0;i<Math.ceil(fullLength/(double)PIECE_LEN);i++)
					{
						String w = (String)obj.get("worker");
						
						//Skip if it's a just-received piece
						if(i == no && receiver.equals(w)) 
						{
							continue;
						}
						
						
						reqPcsColl.insert(new BasicDBObject()
								.append("worker", w)
								.append("name", name)
								.append("no", i));
					}
				}
				if(remove)
				{
					reqPcsColl.remove(new BasicDBObject("name", name).append("no", -1));
				}
			}
			notifyAllUploadThreads();
		}
	}
}
