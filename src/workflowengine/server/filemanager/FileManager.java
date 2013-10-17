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
import difsys.PieceContent;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
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
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import lipermi.net.Client;
import net.fusejna.FuseException;
import workflowengine.schedule.Schedule;
import workflowengine.server.SiteManager;
import workflowengine.server.WorkflowExecutor;
import workflowengine.server.WorkflowExecutorInterface;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class FileManager implements FileManagerInterface
{

	private static int portShift = 100;
	private HashMap<String, Object> locks = new HashMap<>();
	private static FileManager instant;
	private int PIECE_LEN = Utils.getIntProp("fs_piece_size");
	private WorkflowExecutorInterface manager;
	private WorkflowExecutor thisSite;
	private String thisURI = WorkflowExecutor.get().getURI();
	private final HashMap<String, Thread> uploadThreads = new HashMap<>();
//	private final Object PIECE_EXIST_LOCK = new Object();
	private final Object PIECE_RETRIEVED_LOCK = new Object();
	private final Object TRANSFER_QUEUE_LOCK = new Object();
	private DBCollection reqPcsColl;
	private DBCollection fPrtColl;
	private DBCollection exisPcsColl;
	private DBCollection tferQColl;
	private DBCollection uinfRetPcsColl;
	private DBCollection readyFileColl;
	
	private Set<String> peers = new HashSet<>();
	//TODO: create index on mongodb

	protected FileManager() throws FuseException
	{
		Utils.registerRMIServer(FileManagerInterface.class, this, portShift + Utils.getIntProp("local_port"));
		//Init MongoDB
		try
		{
			DB db = new Mongo(
					Utils.getProp("fs_db_host"), 
					Utils.getIntProp("fs_db_port"))
					.getDB(Utils.getProp("fm_db_name")+"_"+thisURI.replace('.', '_').replace(':', '_'));
			db.dropDatabase();
			db = new Mongo(
					Utils.getProp("fs_db_host"), 
					Utils.getIntProp("fs_db_port"))
					.getDB(Utils.getProp("fm_db_name")+"_"+thisURI.replace('.', '_').replace(':', '_'));
			reqPcsColl = db.getCollection("required_pieces");
			fPrtColl = db.getCollection("file_priority");
			exisPcsColl = db.getCollection("existing_pieces");
			tferQColl = db.getCollection("transfer_queue");
			uinfRetPcsColl = db.getCollection("uninformed_retrived_pieces");
			readyFileColl = db.getCollection("ready_files");
			
		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
		}


		//Get site manager
		manager = WorkflowExecutor.getSiteManager();
		thisSite = WorkflowExecutor.get();
		if(manager != null)
		{
			peers.add(Utils.getProp("manager_host")+":"+Utils.getProp("manager_port"));
		}
		
		new difsys.Difsys(Utils.getProp("working_dir"), "default.properties");
		Utils.bash("rm -rf "+Utils.getProp("working_dir")+"/*", false);
		Utils.bash("rm -rf "+Utils.getProp("fs_storage_dir")+"/*", false);
		startListeningThread();
		startUploadThread();
	}

	private void startUploadThread()
	{
		if (manager != null)
		{
			for (final String uri : peers)
			{
				workerJoined(uri);
			}
		}
		if (thisSite instanceof SiteManager)
		{
			Set<String> workers = thisSite.getWorkerSet();
			if (workers != null)
			{
				for (final String uri : workers)
				{
					workerJoined(uri);
				}
			}
		}
	}

	@Override
	public void workerJoined(final String uri)
	{
		synchronized (uploadThreads)
		{
			if (uploadThreads.get(uri) == null)
			{
				Thread t = new Thread("Upload thread for "+uri)
				{
					@Override
					public void run()
					{
						uploadPiece(uri);
					}
				};
				System.out.println("Starting Thread: "+t.getName());
				t.start();
				uploadThreads.put(uri, t);
			}
		}
	}

	/**
	 * Called by site manager only
	 *
	 * @param uri
	 */
	public void broadcaseWorkerJoined(String uri)
	{
		workerJoined(uri);
		if (thisSite instanceof SiteManager)
		{
			Set<String> workers = thisSite.getWorkerSet();
			if (workers != null)
			{
				workers.remove(uri);
				for (final String w : workers)
				{
					getRemoteFileManager(w).workerJoined(uri);
				}
			}
		}
	}
	
	private void addRequiredPiecesToQueue(String toURI)
	{
		DBCursor cursor = reqPcsColl.find(new BasicDBObject("worker", toURI));
		while (cursor.hasNext())
		{
			DBObject obj = cursor.next();
			addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
		}
	}

	private void uploadPiece(String toURI)
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
				{
				}
			}
		}
		while (true)
		{
			//Update retrieved pieces
			List<DBObject> retrieved = getRemoteFileManager(toURI).getRetrievedPieces(thisURI);
			
			for (DBObject obj : retrieved)
			{
				obj.removeField("_id");
				reqPcsColl.remove(obj);
				tferQColl.remove(obj);
//				addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
				obj.put("worker", toURI);
				exisPcsColl.insert(obj);
			}



			BasicDBObject toWorkerQuery = new BasicDBObject("worker", toURI);
			BasicDBObject prioritySortQuery = new BasicDBObject("priority", -1);
			
			

			while(tferQColl.count(toWorkerQuery) == 0)
			{
				synchronized(Thread.currentThread())
				{
					try
					{
						Thread.currentThread().wait(5000);
					}
					catch (InterruptedException ex)
					{}
				}
				addRequiredPiecesToQueue(toURI);
				if (tferQColl.count(toWorkerQuery) == 0)
				{
					DBCursor cursor = exisPcsColl.find(new BasicDBObject("worker", thisURI));
					while (cursor.hasNext())
					{
						DBObject obj = cursor.next();
						addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
					}
				}
			}
			DBObject nextQueueItem = tferQColl.find(toWorkerQuery)
					.sort(prioritySortQuery)
					.limit(1).next();
			PieceInfo p = PieceInfo.get((String) nextQueueItem.get("name"), 
					(int) nextQueueItem.get("no"), -1.0);
			p.fileLength = (long)nextQueueItem.get("full_file_length");
			
			
			//Transfer file to toURI
			try
			{
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
					
					DifsysFile file = DifsysFile.get("/"+p.name, false, false);
					if(file != null)
					{
						content = file.getPieceContent(p.pieceNo).getContent();
						int totalPcs = (int)Math.ceil(p.fileLength/(double)PIECE_LEN);
						if(p.pieceNo < totalPcs-1)
						{
							len = PIECE_LEN;
						}
						else
						{
							len = (int)(p.fileLength % PIECE_LEN);
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
//					System.out.print("Sending "
//							+ p.name.split("/")[1] + "."+p.pieceNo+ " to "+toURI+ " ...");
					
					s.getOutputStream().write(content, 0, len);
//					System.out.println("Done.");
				}
				s.close();
				tferQColl.remove(nextQueueItem);
				nextQueueItem.removeField("_id");
				nextQueueItem.removeField("priority");
				nextQueueItem.removeField("full_file_length");
				reqPcsColl.remove(nextQueueItem);
				
				exisPcsColl.insert(new BasicDBObject()
						.append("name", nextQueueItem.get("name"))
						.append("no", nextQueueItem.get("no"))
						.append("worker", toURI));
			}
			catch (IOException | NumberFormatException e)
			{
				thisSite.logger.log("Cannot send filepiece to " + toURI +": "+e.getMessage(), e);
			}
			System.gc();
		}
	}
	
	protected void pieceRetrieved(PieceInfo p, String fromWorker)
	{
		reqPcsColl.remove(new BasicDBObject()
				.append("name", p.name)
				.append("no", p.pieceNo));

		BasicDBObject existingPcsObj = new BasicDBObject()
				.append("name", p.name)
				.append("no", p.pieceNo)
				.append("full_file_length", p.fileLength)
				.append("worker", thisURI);
		exisPcsColl.insert(existingPcsObj);
		existingPcsObj.append("worker", fromWorker);
		exisPcsColl.insert(existingPcsObj);

		notifyAllUploadThreads();
		if(p.fileLength < 1)
		{
			System.out.println("Warning: Piece length less than 1.");
			checkIfAllPieceReceived(p.name);
		}
		else
		{
			checkIfAllPieceReceived(p.name, p.fileLength);
		}
		
		synchronized(PIECE_RETRIEVED_LOCK)
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
		for(final Thread t : uploadThreads.values())
		{
			synchronized(t)
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
		if(obj != null)
		{
			checkIfAllPieceReceived(name, (long)obj.get("full_file_length"));
		}
	}
	
	public void checkIfAllPieceReceived(String name, long len)
	{
		if(len < 1)
		{
			throw new IllegalArgumentException("Length is less than 1");
		}
		boolean ready = true;
		for (int i = 0; i < Math.ceil(len / (double) PIECE_LEN); i++)
		{
			long count = exisPcsColl.count(new BasicDBObject()
					.append("name", name)
					.append("no", i)
					.append("worker", thisURI));
			if (count == 0)
			{
				ready = false;
				break;
			}
		}
		
		if (ready)
		{
			readyFileColl.insert(new BasicDBObject("name", name));
			Utils.mkdirs(Utils.getParentPath(Utils.getProp("working_dir")+"/"+name));
			DifsysFile.addFile("/"+name, len);
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
	public void setFilePriority(List<DBObject> list)
	{
		for (DBObject obj : list)
		{
			obj.removeField("_id");
			fPrtColl.update(new BasicDBObject("name", obj.get("name")), obj, true, false);
		}
		updateQueuePriority();
	}

	private void updateQueuePriority()
	{
		
		DBCursor c = tferQColl.find();
		while(c.hasNext())
		{
			DBObject obj = c.next();
			addToQueue((String)obj.get("name"), (int)obj.get("no"), (String)obj.get("worker"));
		}
		
	}
	
	private void addToQueue(String name)
	{
		for(String target : peers)
		{
			addToQueue(name, -1, target);
		}
	}
	
	
	/**
	 * Add filepiece to transfer queue.
	 * If filepiece is already in the queue, the priority is updated
	 * @param reqPiece must have field name, no
	 */
	private void addToQueue(String name, int no, String targetWorker)
	{
		BasicDBObject pcsQuery = new BasicDBObject("name", name);
		BasicDBObject extPcsQuery = new BasicDBObject("name", name).append("worker", thisURI);
		
		if(no != -1)
		{
			boolean targetPcsExists = exisPcsColl.count(
							new BasicDBObject("name", name)
							.append("no", no)
							.append("worker", targetWorker)
							)>0;
			if(targetPcsExists)
			{
				return;
			}
			pcsQuery.append("no", no);
			extPcsQuery.append("no", no);
		}
		boolean pieceExists = exisPcsColl.count(extPcsQuery) > 0;
		
		if(pieceExists)
		{
			
			double priority = 1;
			DBObject priorityObj = fPrtColl.findOne(
					new BasicDBObject("name", name));
			if(priorityObj != null)
			{
				priority = (double)priorityObj.get("priority");
			}
			
			if(no == -1)
			{
				DBCursor cursor = exisPcsColl.find(extPcsQuery);
				while(cursor.hasNext())
				{
					DBObject obj = cursor.next();
					no = (int)obj.get("no");
					boolean targetPcsExists = exisPcsColl.count(
							new BasicDBObject("name", name)
							.append("worker", targetWorker)
							.append("no", no)
							)>0;
					boolean targetNeeded = reqPcsColl.count(
							new BasicDBObject("name", name)
							.append("worker", targetWorker)) > 0;
					pcsQuery.append("no", no);
					int reqSites = (int) reqPcsColl.count(pcsQuery);
					if(!targetPcsExists)
					{
						tferQColl.update(
							new BasicDBObject()
							.append("worker", targetWorker)
							.append("name", name)
							.append("no", no),
							new BasicDBObject()
							.append("worker", targetWorker)
							.append("name", name)
							.append("no", no)
							.append("priority", calTransferPriority(priority, targetNeeded, reqSites))
							.append("full_file_length", obj.get("full_file_length")),
							true, false);
					}
				}
			}
			else
			{
				boolean targetNeeded = reqPcsColl.count(new BasicDBObject("name", name)
					.append("worker", targetWorker)
					.append("no", no)) > 0;
				int reqSites = (int) reqPcsColl.count(pcsQuery);
				DBObject obj = exisPcsColl.findOne(extPcsQuery);
				tferQColl.update(
					new BasicDBObject()
					.append("worker", targetWorker)
					.append("name", name)
					.append("no", no),
					new BasicDBObject()
					.append("worker", targetWorker)
					.append("name", name)
					.append("no", no)
					.append("priority", calTransferPriority(priority, targetNeeded, reqSites))
					.append("full_file_length", obj.get("full_file_length")),
					true, false);
			
			}
		}
		System.gc();
	}

	private double calTransferPriority(double filePriority, boolean isRequired, int requiringSites)
	{
		return filePriority * (requiringSites + 1) + 1 + ((isRequired?0:1)*filePriority);
	}
	
	@Override
	public void setAllRequiredPieces(String invoker, List<DBObject> list)
	{
		if (!invoker.equals(thisURI))
		{
			reqPcsColl.insert(list);
		}

		for (DBObject obj : list)
		{
			addToQueue((String) obj.get("name"), (int) obj.get("no"), (String) obj.get("worker"));
		}
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
		synchronized(PIECE_RETRIEVED_LOCK)
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
						.append("full_file_length", (long)file.getSize())
						.append("worker", thisURI));
			}
			readyFileColl.insert(new BasicDBObject("name", file.getName(wfid)));
		}


		List<DBObject> reqPcs = reqPcsColl.find().toArray();
		List<DBObject> fPrt = fPrtColl.find().toArray();
		setAllRequiredPieces(thisURI, reqPcs);
		for (String uri : thisSite.getWorkerSet())
		{
			FileManagerInterface fm = getRemoteFileManager(uri);
			fm.setFilePriority(fPrt);
			fm.setAllRequiredPieces(thisURI, reqPcs);
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
							Logger.getLogger(FileManager.class.getName()).log(Level.SEVERE, null, ex);
						}
					}
				}
			}.start();
		}
		catch (IOException ex)
		{
			Logger.getLogger(FileManager.class.getName()).log(Level.SEVERE, null, ex);
		}

	}

	protected void uploadRequestAccepted(final Socket s)
	{
		new Thread()
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
					Logger.getLogger(FileManager.class.getName()).log(Level.SEVERE, null, ex);
				}
				System.gc();
			}
		}.start();
	}

	public static FileManagerInterface getRemoteFileManager(String weURI)
	{
		FileManagerInterface fm = null;
		int tries = 0;
		String[] s = weURI.split(":");
		String uri = s[0] + ":" + (portShift + Integer.parseInt(s[1]));
		while(fm == null && tries < 10)
		{
			try
			{
				Client c = Utils.getRMIClient(uri);
				fm = (FileManagerInterface) c.getGlobal(FileManagerInterface.class);
			}
			catch (Exception e)
			{
				fm = null;
				tries ++;
				try
				{
					Thread.sleep(1000);
				}
				catch (InterruptedException ex)
				{}
			}
		}
		return fm;
	}

	public static FileManager get()
	{
		if (instant == null)
		{
			try
			{
				instant = new FileManager();
			}
			catch (FuseException ex)
			{}
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

	public void outputCreated(String fname)
	{
		
		
		long size = new File(Utils.getProp("working_dir") + "/" + fname).length();
		for (int i = 0; i < Math.ceil(size / (double) PIECE_LEN); i++)
		{
//			existingPieces.add(
//					PieceInfo.get(fname, i, wff.getPriority(), size));
			exisPcsColl.insert(new BasicDBObject().append("name", fname).append("no", i)
					.append("worker", thisURI)
					.append("full_file_length", size));
		}
		readyFileColl.insert(new BasicDBObject().append("name", fname));
		addToQueue(fname);
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
	
	
	@Override
	public void setPeerSet(Set<String> workers)
	{
		this.peers = new HashSet<>(workers);
		this.peers.remove(thisURI);
		for(String p : peers)
		{
			workerJoined(p);
		}
	}
	/**
	 * Called by SiteManager only!
	 * @param workers 
	 */
	public void broadcastPeerSet(final Set<String> workers)
	{
		setPeerSet(workers);
		
		new Thread()
		{
			@Override
			public void run()
			{
				for (String w : WorkflowExecutor.get().getWorkerSet())
				{
					FileManagerInterface fm = null;
					while (fm == null)
					{
						fm = FileManager.getRemoteFileManager(w);
					}
					fm.setPeerSet(workers);
				}
			}
		}.start();
	}
}
