/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import workflowengine.schedule.Schedule;
import workflowengine.server.WorkflowExecutor;
import workflowengine.server.filemanager.FileManager;
import workflowengine.utils.LockStorage;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class NewDIFM extends FileManager implements PeerInterface, Runnable
{

	public static double END_GAME_MODE_RATIO = 95.0;
	public static int PIECE_LEN = 500 * 1024; //500K
	private static NewDIFM INSTANCE;
	private final Map<String, Set<String>> wantingFiles; // peer -> set of wanting files
	private final Map<String, Set<String>> wantingPeers; // file -> set of wanting peers
	private final Map<String, DIFMFile> fileMap;
	private final SortedSet<DIFMFile> fileQueue;
//	private final Queue<PeerInterface> peers;
//	private final List<String> peerURIs;
	private final Queue<String> peerQueue;
	private final Map<String, PeerInterface> peerMap;
	private final String thisURI;
	private long transferredBytes;
	private boolean stopping;
	private final List<Thread> downloadingThreads;
	private final Map<String, String> fileWaitingLocks;
	
	private final Map<String, String> fileInfoWaitingLocks;

	private NewDIFM()
	{
		Utils.setPropIfNotExist("DIFM_piece_length", "1000000");
		PIECE_LEN = Utils.getIntProp("DIFM_piece_length");
		thisURI = WorkflowExecutor.get().getURI();
		wantingFiles = new ConcurrentHashMap<>();
		wantingPeers = new ConcurrentHashMap<>();
		fileWaitingLocks = new ConcurrentHashMap<>();
		fileInfoWaitingLocks = new ConcurrentHashMap<>();
		fileMap = new ConcurrentHashMap<>();

		fileQueue = Collections.synchronizedSortedSet(new TreeSet<>(new Comparator<DIFMFile>()
		{
			private double calPriority(DIFMFile file)
			{
				Set<String> peersWantingThisFile = wantingPeers.get(file.getName());
				double isRequired = 0;
				if (peersWantingThisFile.contains(thisURI))
				{
					isRequired = 5;
				}
				return file.getPriority() * (peersWantingThisFile.size() + isRequired);
				//+ Math.random();
			}

			@Override
			public int compare(DIFMFile o1, DIFMFile o2)
			{
				if (o1.equals(o2))
				{
					return 0;
				}
				double p1 = calPriority(o1);
				double p2 = calPriority(o2);
				return (p1 > p2) ? -1 : ((p1 < p2) ? 1 : o1.getName().compareTo(o2.getName()));
			}
		}));


//		peers = new ConcurrentLinkedQueue<>();
//		peerURIs = Collections.synchronizedList(new LinkedList<String>());
		peerMap = new ConcurrentHashMap<>();
		peerQueue = new ConcurrentLinkedQueue<>();
		
		transferredBytes = 0;
		stopping = false;
		downloadingThreads = Collections.synchronizedList(new LinkedList<Thread>());
		Utils.bash("rm -rf " + Utils.getProp("working_dir") + "/*");
		DIUtils.registerLocalPeer(thisURI, this);
	}

	public static NewDIFM get()
	{
		if (INSTANCE == null)
		{
			INSTANCE = new NewDIFM();
		}
		return INSTANCE;
	}

	@Override
	public Object processMsg(MsgType t, Object msg, String fromURI)
	{
		System.out.println(t + " is received.");
		switch (t)
		{
			case PEER_CONNECTED:
			{
				String uri = (String) msg;
				PeerInterface newPeer = DIUtils.getPeerFromURI(uri);
				//peers.add(newPeer);
				peerMap.put(uri, newPeer);
				peerQueue.add(uri);
				peerIsConnected(newPeer);
				return null;
			}
			case PEER_LIST:
			{
				String[] uris = (String[]) msg;
				PeerInterface peer;
				for (String uri : uris)
				{
					peer = DIUtils.getPeerFromURI(uri);
				//peers.add(newPeer);
				peerMap.put(uri, peer);
				peerQueue.add(uri);
					peerIsConnected(peer);
				}
				peer = DIUtils.getPeerFromURI(fromURI);
				//peers.add(newPeer);
				peerMap.put(fromURI, peer);
				peerQueue.add(fromURI);
				peerIsConnected(peer);
				return null;
			}
			case FILE_REQ_INFO:
			{
				Map<String, Set<String>>[] maps = (Map<String, Set<String>>[]) msg;
				DIUtils.mergeFileMap(maps[0], wantingFiles);
				DIUtils.mergeFileMap(maps[1], wantingPeers);
				return null;
			}
			case FILE_INFO: //msg -> (Map[])file info (Keys: name, length, priority)
			{
				Map<String, Object>[] fileInfos = (Map<String, Object>[]) msg;
				for (Map<String, Object> fileInfo : fileInfos)
				{
					String name = (String) fileInfo.get("name");
					System.out.println("File info " + name + " received.");
					Set<String> peersWantThisFile = wantingPeers.get(name);
					System.out.println("Required: " + peersWantThisFile.contains(thisURI));
					DIFMFile file = new DIFMFile(name,
							(long) fileInfo.get("length"),
							(double) fileInfo.get("priority"),
							peersWantThisFile.size(),
							peersWantThisFile.contains(thisURI));
					fileMap.put(name, file);
					fileInfoCreated(file);
				}
				DIUtils.notifyDownloadThread(downloadingThreads);
				return null;
			}
			case FILE_INACTIVATE:
			{
				_inactivateFile(fileMap.get((String) msg));
				return null;
			}
		}
		return null;
	}

	/**
	 * Called after new file info is received from remote peer
	 *
	 * @param file
	 */
	private void fileInfoCreated(DIFMFile file)
	{
		String lock = fileInfoWaitingLocks.get(file.getName());
		if(lock != null)
		{
			synchronized(lock)
			{
				lock.notifyAll();
			}
		}
		
		if (file.isRequired() && !file.isCompleted())
		{
			fileQueue.add(file);
		}
	}

	private Object[] broadcast(final MsgType t, final Object msg, boolean wait)
	{
		if (wait)
		{
			return _doBroadcast(t, msg);
		}
		else
		{
			Thread thread = new Thread()
			{
				@Override
				public void run()
				{
					_doBroadcast(t, msg);
				}
			};
			thread.start();
			return null;
		}
	}

	private Object[] _doBroadcast(MsgType t, Object msg)
	{
//		Object[] res = new Object[peers.size()];
		Object[] res = new Object[peerMap.size()];
		int i = 0;
		for (PeerInterface p : peerMap.values())
		{
			res[i] = p.processMsg(t, msg, thisURI);
			i++;
		}
		return res;
	}

	private void peerIsConnected(PeerInterface peer)
	{
		System.out.println("Peer " + peer.toString() + " is connected.");
		Thread t = new Thread(this, "Download thread from " + peer.toString());
		downloadingThreads.add(t);
		t.start();
	}
	
	private DIFMFile getDIFMFile(String filename)
	{
		DIFMFile f = fileMap.get(filename);
		while(f == null)
		{
			String lock = fileInfoWaitingLocks.get(filename);
			if(lock == null)
			{
				lock = filename;
				fileInfoWaitingLocks.put(filename, filename);
			}
			synchronized(lock)
			{
				try
				{
					lock.wait(5000);
				}
				catch (InterruptedException ex)
				{}
			}
			f = fileMap.get(filename);
		}
		return f;
	}

	@Override
	public AtomicBitSet getExistingPcs(String file)
	{
		return getDIFMFile(file).getExisting();
	}

	
	private Piece getPieceToDownload(String targetURI, PeerInterface target)
	{

		//Select a file to download based on priority
		DIFMFile file = DIUtils.getFileProportionally(fileQueue, 10);
		Piece p = null;
		if (file != null)
		{
			//Get existing pieces and interesting pieces from remote peer
			AtomicBitSet interesting = _getInterestingPcs(targetURI, target, file, false);
			p = file.getPieceToDownload(interesting);
		}
		return p;
	}
	
	private Map<String, AtomicBitSet> existingPcsCache = new ConcurrentHashMap<>();
	private AtomicBitSet _getInterestingPcs(String targetURI, PeerInterface target, DIFMFile file, boolean force)
	{
		//Get remote existing pieces from cache if possible
		AtomicBitSet remoteExisting = existingPcsCache.get(targetURI+file.getName());
		if(force || remoteExisting == null || Math.random() < 0.1)
		{
			remoteExisting = target.getExistingPcs(file.getName());
			file.incrementPieceSeen(remoteExisting);
			existingPcsCache.put(targetURI+file.getName(), remoteExisting);
		}
		
		AtomicBitSet interesting = (AtomicBitSet) remoteExisting.clone();
		interesting.andNot(file.getExisting());
		interesting.andNot(file.getRequesting());
		
		//If no interesting piece, force to get remote existing pieces and 
		//get interesting bitset again
		if(!force && interesting.cardinality() == 0)
		{
			interesting = _getInterestingPcs(targetURI, target, file, true);
		}
		
		return interesting;
	}

	/**
	 * Finalize piece information after the piece is downloaded
	 *
	 * @param p
	 */
	private void pieceIsDownloaded(Piece p)
	{
		DIFMFile file = fileMap.get(p.name);

		//Update file priority
		fileQueue.remove(file);
		fileQueue.add(file);


		if (file.isCompleted())
		{
			fileIsDownloaded(file);
		}
		transferredBytes += file.getPieceLength(p.index);
	}

	public void workerConnected(String uri)
	{
		PeerInterface newPeer = DIUtils.getPeerFromURI(uri);
		if (newPeer != null)
		{
//			synchronized (peers)
			synchronized (peerMap)
			{
				broadcast(MsgType.PEER_CONNECTED, uri, true);
//				String[] peerList = new String[peerURIs.size()];
				String[] peerList = new String[peerMap.size()];
				newPeer.processMsg(MsgType.PEER_LIST, peerMap.keySet().toArray(peerList), thisURI);

//				peers.add(newPeer);
//				peerURIs.add(uri);
				peerMap.put(uri, newPeer);
				peerQueue.add(uri);
				peerIsConnected(newPeer);
			}
		}
	}


	/**
	 * Called when the file is completely downloaded
	 *
	 * @param name
	 */
	private void fileIsDownloaded(DIFMFile file)
	{
		System.out.println(file.getName().substring(37) + " is downloaded.");
		file.setRequired(false);
		file.finallized();
		fileQueue.remove(file);

		Object lock = fileWaitingLocks.get(file.getName());
		System.out.println(lock == null);
		if (lock != null)
		{
			synchronized (lock)
			{
				lock.notifyAll();
			}
		}
	}

	/**
	 * Inactivate the file
	 *
	 * @param file
	 */
	private void _inactivateFile(DIFMFile file)
	{
		file.setInactive();
		fileQueue.remove(file);
		//fileMap.remove(file.getName());

		//TODO: remove file from filemap (optional)
	}

	public void inactivateFile(String filename)
	{
		System.out.println("Inactivating " + filename);
		DIFMFile file = fileMap.get(filename);
		if(file.isActive())
		{
			_inactivateFile(fileMap.get(filename));
			broadcast(MsgType.FILE_INACTIVATE, filename, false);
		}
	}

	/**
	 * called when new file is created
	 *
	 * @param name
	 * @param length
	 * @param priority
	 */
	private void fileCreated(String name, long length, double priority)
	{
		Set<String> peersWantThisFile = wantingPeers.get(name);
		boolean requiredByThisPeer;
		int wantingPeerCount;
		if (peersWantThisFile == null)
		{
			requiredByThisPeer = false;
			wantingPeerCount = 0;
		}
		else
		{
			requiredByThisPeer = peersWantThisFile.contains(thisURI);
			wantingPeerCount = peersWantThisFile.size();
		}

		DIFMFile file = new DIFMFile(name, length, priority,
				wantingPeerCount, requiredByThisPeer);
		fileMap.put(name, file);
		fileInfoCreated(file);
		file.setComplete();
		fileIsDownloaded(file);
	}

	private double start = Utils.time();
	private double downloaded = 0;
	private void download()
	{
		String targetURI;
		synchronized (peerQueue)
		{
			targetURI = peerQueue.poll();
			peerQueue.add(targetURI);
		}
//		final Lock targetLock = LockStorage.getLock("PEER_TO_DOWNLOAD", targetURI);
//		if (targetLock.tryLock())
//		{
			PeerInterface target = peerMap.get(targetURI);
			Piece p = getPieceToDownload(targetURI, target);
			if (p != null)
			{
				final Lock pieceLock = LockStorage.getLock("PIECE_TO_DOWNLOAD", p);
				if (pieceLock.tryLock())
				{
					try
					{
						DIFMFile file = fileMap.get(p.name);
						if (!file.getExisting().get(p.index))
						{
	//						System.out.println("Downloading "
	//								+ p.name.substring(37)
	//								+ "(" + p.index + ") from "
	//								+ target.toString() + "...");
							file.setRequesting(p.index, true);
							byte[] content = target.getPieceContent(p.name, p.index);
							file.write(p.index, content, file.getPieceLength(p.index));
							pieceIsDownloaded(p);
							file.setRequesting(p.index, false);
	//						System.out.printf("Remain %d pcs\n",
	//								file.getRemainingPcs());
							downloaded += file.getPieceLength(p.index);

							System.out.printf("%s(%.0f%%) - %.2fMBps %d  \r", 
									p.name.substring(37),
									file.getPercentDownloaded(),
									downloaded/(Utils.time()-start)/1024.0/1024.0,
									p.index);
	//						System.out.println("Done.");
						}
					}
					finally
					{
						pieceLock.unlock();
					}
				}
			}
//			targetLock.unlock();
//		}
		System.gc();
	}

	/**
	 * Get the content of the piece to downloading peer
	 *
	 * @param name
	 * @param index
	 * @return
	 */
	@Override
	public byte[] getPieceContent(String name, int index)
	{
//		System.out.println("Sending "+name.substring(37)+"("+index+")");
		DIFMFile file = fileMap.get(name);
		int length = file.getPieceLength(index);
		byte[] buffer = file.read(index, length);
		file.seenPiece(index);
		transferredBytes += length;
		return buffer;
	}

	/**
	 * Store information of schedule
	 *
	 * @param s
	 */
	public void setSchedule(Schedule s)
	{
		Set<String> tasks = s.getSettings().getTaskUUIDSet();
		String wfid = s.getWorkflowID();
		for (String tid : tasks)
		{
			Task t = Task.get(tid);
			String worker = s.getWorkerForTask(tid);

			//Add wantingFiles and wantingPeers
			for (String fid : t.getInputFiles())
			{
				_setFilePeerRequirement(WorkflowFile.get(fid).getName(wfid), worker);
			}
		}

		//Set to transfer workflow's output files to this peer
		for (String fid : s.getSettings().getWorkflow().getOutputFiles())
		{
			_setFilePeerRequirement(WorkflowFile.get(fid).getName(wfid), thisURI);
		}

		//Broadcast file requirement information
		broadcast(MsgType.FILE_REQ_INFO, new Map[]
		{
			wantingFiles, wantingPeers
		}, true);

		//Notify the file manager that the files are created
		outputFilesCreated(wfid, s.getSettings().getWorkflow().getInputFiles());
	}

	private void _setFilePeerRequirement(String filename, String peerURI)
	{
		Set<String> wantingFilesOfWorker = wantingFiles.get(peerURI);
		if (wantingFilesOfWorker == null)
		{
			wantingFilesOfWorker = Collections.synchronizedSet(new HashSet<String>());
			wantingFiles.put(peerURI, wantingFilesOfWorker);
		}
		wantingFilesOfWorker.add(filename);

		Set<String> wantingWorkersOfFile = wantingPeers.get(filename);
		if (wantingWorkersOfFile == null)
		{
			wantingWorkersOfFile = Collections.synchronizedSet(new HashSet<String>());
			wantingPeers.put(filename, wantingWorkersOfFile);
		}
		wantingWorkersOfFile.add(peerURI);
	}

	/**
	 * Block until the file is completely downloaded
	 *
	 * @param name
	 */
	@Override
	public void waitForFile(String name)
	{
		System.out.println("Waiting for " + name.substring(37));
		Object lock = fileWaitingLocks.get(name);
		if (lock == null)
		{
			lock = name;
			fileWaitingLocks.put(name, name);
		}

		DIFMFile file = fileMap.get(name);
		synchronized (lock)
		{
			while (file == null)
			{
				try
				{
					lock.wait(5000);
				}
				catch (InterruptedException ex)
				{
				}
				file = fileMap.get(name);
			}
			if (!file.isCompleted())
			{
				while (!file.isCompleted())
				{
					try
					{
						lock.wait(5000);
					}
					catch (InterruptedException ex)
					{
					}
				}
			}
		}

		System.out.println("Done waiting for " + name.substring(37));
	}

	/**
	 * Called when new file is created
	 *
	 * @param fname
	 */
	@Override
	public void outputFilesCreated(String wfid, Set<String> files)
	{
		LinkedList<Map<String, Object>> fileToBroadcast = new LinkedList<>();
		for (String fid : files)
		{
			WorkflowFile f = WorkflowFile.get(fid);
			String fname = f.getName(wfid);
			long length = Utils.getFileLength(Utils.getProp("working_dir") + "/" + fname);
			fileCreated(fname, length, f.getPriority());

			Set<String> peersWantingThisFile = wantingPeers.get(fname);
			if (peersWantingThisFile != null
					&& !(peersWantingThisFile.size() == 1
					&& peersWantingThisFile.contains(thisURI)))
			{
				HashMap<String, Object> fileInfo = new HashMap<>();
				fileInfo.put("name", fname);
				fileInfo.put("length", length);
				fileInfo.put("priority", f.getPriority());
				fileToBroadcast.add(fileInfo);
			}
		}

		if (!fileToBroadcast.isEmpty())
		{
			Map<String, Object>[] fileInfo = fileToBroadcast.toArray(new Map[fileToBroadcast.size()]);
			broadcast(MsgType.FILE_INFO, fileInfo, false);
		}
	}

	@Override
	public void shutdown()
	{
		stopping = true;
	}

	@Override
	public long getTransferredBytes()
	{
		return transferredBytes;
	}

	/**
	 * Runnable implementation for download threads
	 */
	@Override
	public void run()
	{

		while (!stopping)
		{
			try
			{
				//Wait until a file is available
				while (fileQueue.isEmpty() && !stopping)
				{
					synchronized (this)
					{
						try
						{
							this.wait(10000);
						}
						catch (InterruptedException ex)
						{
						}
					}
				}

				if (!stopping)
				{
					this.download();
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public String toString()
	{
		return thisURI;
	}

	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 59 * hash + Objects.hashCode(this.thisURI);
		return hash;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		final NewDIFM other = (NewDIFM) obj;
		if (!Objects.equals(this.thisURI, other.thisURI))
		{
			return false;
		}
		return true;
	}
	
	
}
