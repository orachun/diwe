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
//	private final HashMap<String, Double> filePriority = new HashMap<>();
//	private final Set<PieceInfo> existingPieces = new HashSet<>();
//	private Map<String, Set<PieceInfo>> requiredPieces = new HashMap<>();
//	private Map<String, Set<String>> requiringSites = new HashMap<>();
//	private HashMap<String, Set<PieceInfo>> siteExistingPieces = new HashMap<>();
//	private Set<String> readyFiles = new HashSet<>();
//	private HashMap<String, TransferQueue> transferQ = new HashMap<>();
	private final HashMap<String, Thread> uploadThreads = new HashMap<>();
	private final Object PIECE_EXIST_LOCK = new Object();
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

	private void uploadPiece(String toURI)
	{
		while (exisPcsColl.count(new BasicDBObject("worker", thisURI)) == 0)
		{
			synchronized (PIECE_EXIST_LOCK)
			{
				try
				{
					PIECE_EXIST_LOCK.wait();
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
//			pieces.removeAll(retrieved);
			for (DBObject obj : retrieved)
			{
				obj.removeField("_id");
				reqPcsColl.remove(obj);
				tferQColl.remove(obj);
				addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
			}



			BasicDBObject toWorkerQuery = new BasicDBObject("worker", toURI);
			BasicDBObject prioritySortQuery = new BasicDBObject("priority", -1);
			
			if (tferQColl.count(toWorkerQuery) == 0)
			{
				DBCursor cursor = exisPcsColl.find(new BasicDBObject("worker", thisURI));
				while (cursor.hasNext())
				{
					DBObject obj = cursor.next();
					addToQueue((String) obj.get("name"), (int) obj.get("no"), toURI);
				}
			}
			while(tferQColl.count(toWorkerQuery) == 0)
			{
				synchronized(PIECE_EXIST_LOCK)
				{
					try
					{
						PIECE_EXIST_LOCK.wait(5000);
					}
					catch (InterruptedException ex)
					{}
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
					//System.out.println("Sending " + len+" bytes of "
					//		+ p.name.split("/")[1] + "."+p.pieceNo+ " to "+toURI+ " ...");
					
					s.getOutputStream().write(content, 0, len);
					//System.out.println("Done.");
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
//	private Map<String, Set<PieceInfo>> uninformedRetrievedPieces = new HashMap<>();

	protected void pieceRetrieved(PieceInfo p, String fromWorker)
	{
		for (String uri : peers)
		{
			uinfRetPcsColl.insert(new BasicDBObject()
					.append("name", p.name)
					.append("no", p.pieceNo)
					.append("worker", uri));
		}

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

		synchronized (PIECE_EXIST_LOCK)
		{
			PIECE_EXIST_LOCK.notifyAll();
		}

		checkIfAllPieceReceived(p.name, p.fileLength);
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
	}

	/**
	 *
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
			
			double priority = 0;
			try{
			priority = (double) fPrtColl.findOne(
					new BasicDBObject("name", name)).get("priority");
			}
			catch(NullPointerException e)
			{
				System.out.println(name);
				System.exit(1);
			}
			if(no == -1)
			{
				DBCursor cursor = exisPcsColl.find(extPcsQuery);
				while(cursor.hasNext())
				{
					int reqSites = (int) reqPcsColl.count(pcsQuery);
					DBObject obj = cursor.next();
					no = (int)obj.get("no");
					boolean targetPcsExists = exisPcsColl.count(
							new BasicDBObject("name", name)
							.append("worker", targetWorker)
							.append("no", no)
							)>0;
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
							.append("priority", priority * (reqSites + 1) + 1)
							.append("full_file_length", obj.get("full_file_length")),
							true, false);
					}
				}
			}
			else
			{
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
					.append("priority", priority * (reqSites + 1) + 1)
					.append("full_file_length", obj.get("full_file_length")),
					true, false);
			
			}
		}
		System.gc();
	}

	@Override
//	public void setAllRequiredPieces(Map<String, Set<PieceInfo>> requiredPieces,
//			Map<String, Set<String>> requiringSites)
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

//		this.requiredPieces = requiredPieces;
//		this.requiringSites = requiringSites;
//		for (Map.Entry<String, Set<PieceInfo>> e : requiredPieces.entrySet())
//		{
//			TransferQueue q = transferQ.get(e.getKey());
//			if (q == null)
//			{
//				q = new TransferQueue();
//				transferQ.put(e.getKey(), q);
//			}
//			for (PieceInfo p : e.getValue())
//			{
//				Set<String> sites = requiringSites.get(p.name);
//				if(sites == null)
//				{
//					System.out.println(p.name);
//				}
//				int totalRequiringSizes = sites.size();
//				q.add(p, totalRequiringSizes);
//			}
//		}
	}

	@Override
	public List<DBObject> getRetrievedPieces(String invoker)
	{
//		Set<PieceInfo> s = uninformedRetrievedPieces.get(invoker);
//		if (s == null)
//		{
//			return new HashSet<>();
//		}
//		uninformedRetrievedPieces.put(invoker, new HashSet<PieceInfo>());
//		return s;
		return uinfRetPcsColl.find(new BasicDBObject("worker", invoker)).toArray();
	}

	/**
	 * Record that which files are required by which sites. Also record that all
	 * input file of workflow is existing
	 *
	 * @param s
	 */
	public void setSchedule(Schedule s)
	{
		String wfid = s.getWorkflowID();
		Workflow wf = s.getSettings().getWorkflow();

		//Set file requirements of input files of all tasks
		for (String tid : wf.getTaskSet())
		{
			Task t = Task.get(tid);
			String worker = s.getWorkerForTask(tid);
//			Set<PieceInfo> reqPcs = requiredPieces.get(wkid);
//			if (reqPcs == null)
//			{
//				reqPcs = new HashSet<>();
//				requiredPieces.put(wkid, reqPcs);
//			}
//			for (String fid : t.getInputFiles())
//			{
//				WorkflowFile file = WorkflowFile.get(fid);
//				reqPcs.add(PieceInfo.get(file.getName(wfid), -1, file.getPriority()));
//				Set<String> siteSet = requiringSites.get(file.getName(wfid));
//				if (siteSet == null)
//				{
//					siteSet = new HashSet<>();
//					requiringSites.put(file.getName(wfid), siteSet);
//				}
//				siteSet.add(wkid);
//				filePriority.put(file.getName(wfid), file.getPriority());
//			}
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

		//Set workflow output file requirement to send back
//		Set<PieceInfo> thisFileSet = requiredPieces.get(thisURI);
//		if (thisFileSet == null)
//		{
//			thisFileSet = new HashSet<>();
//			requiredPieces.put(thisURI, thisFileSet);
//		}
//		
		for (String fid : wf.getOutputFiles())
		{
//			WorkflowFile file = WorkflowFile.get(fid);
//			thisFileSet.add(PieceInfo.get(file.getName(wfid), -1, file.getPriority()));
//			filePriority.put(file.getName(wfid), file.getPriority());
//			Set<String> siteSet = requiringSites.get(file.getName(wfid));
//			if (siteSet == null)
//			{
//				siteSet = new HashSet<>();
//				requiringSites.put(file.getName(wfid), siteSet);
//			}
//			siteSet.add(thisURI);
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

		synchronized (PIECE_EXIST_LOCK)
		{
			PIECE_EXIST_LOCK.notifyAll();
		}
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

	public void waitForFile(WorkflowFile wff, String workflowDirName)
	{
		String fname = workflowDirName + "/" + wff.getName();
//		if(Utils.fileExists(Utils.getProp("working_dir")+"/"+fname) || readyFiles.contains(fname))
		if (Utils.fileExists(Utils.getProp("working_dir") + "/" + fname)
				|| readyFileColl.count(new BasicDBObject("name", fname)) > 0)
		{
			return;
		}
		locks.put(fname, fname);
		synchronized (fname)
		{
//			while (!readyFiles.contains(fname))
			while (!Utils.fileExists(Utils.getProp("working_dir") + "/" + fname)
				&& readyFileColl.count(new BasicDBObject("name", fname)) == 0)
			{
				try
				{
					fname.wait(5000);
				}
				catch (InterruptedException ex)
				{
				}
				checkIfAllPieceReceived(fname);
			}
		}
		locks.remove(fname);
	}

	public void outputCreated(WorkflowFile wff, String workflowDirName)
	{
		String fname = workflowDirName + "/" + wff.getName();
		long size = new File(Utils.getProp("working_dir") + "/" + fname).length();
		for (int i = 0; i < Math.ceil(size / (double) PIECE_LEN); i++)
		{
//			existingPieces.add(
//					PieceInfo.get(fname, i, wff.getPriority(), size));
			exisPcsColl.insert(new BasicDBObject().append("name", fname).append("no", i)
					.append("worker", thisURI)
					.append("full_file_length", size));
			readyFileColl.insert(new BasicDBObject().append("name", fname));
			BasicDBList or = new BasicDBList();
			or.add(new BasicDBObject("no", i));
			or.add(new BasicDBObject("no", -1));
			DBCursor c = reqPcsColl.find(new BasicDBObject().append("name", fname)
					.append("$or", or));
			while(c.hasNext())
			{
				DBObject obj = c.next();
				addToQueue(fname, i, (String)obj.get("worker"));
			}
		}
		
		synchronized(PIECE_EXIST_LOCK)
		{
			PIECE_EXIST_LOCK.notifyAll();
		}
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
