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
import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import workflowengine.schedule.Schedule;
import workflowengine.server.WorkflowExecutor;
import workflowengine.server.filemanager.FileManager;
import workflowengine.utils.Utils;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class NewDIFM extends FileManager implements PeerInterface, Runnable
{
	private static NewDIFM INSTANCE;
	
	private final Map<String, Set<String>> wantingFiles; // peer -> set of wanting files
	private final Map<String, Set<String>> wantingPeers; // file -> set of wanting peers
	
	private final Map<String, DIFMFile> fileMap;
	private final SortedSet<DIFMFile> fileQueue;
	
	private final Queue<PeerInterface> peers;
	private final List<String> peerURIs;
	private final String thisURI;
	private long transferredBytes;
	private boolean stopping;
	private final List<Thread> downloadingThreads;
	
	
	private final Map<String, String> waitingLocks;

	private NewDIFM()
	{
		thisURI = WorkflowExecutor.get().getURI();
		wantingFiles = new ConcurrentHashMap<>();
		wantingPeers = new ConcurrentHashMap<>();
		waitingLocks = new ConcurrentHashMap<>();
		fileMap = new ConcurrentHashMap<>();
		
		fileQueue = Collections.synchronizedSortedSet(new TreeSet<>(new Comparator<DIFMFile>(){

			private double calPriority(DIFMFile file)
			{
				Set<String> peersWantingThisFile = wantingPeers.get(file.getName());
				double isRequired = 0;
				if(peersWantingThisFile.contains(thisURI))
				{
					isRequired = 5;
				}
				return file.getPriority()*(peersWantingThisFile.size() + isRequired) ;
						//+ Math.random();
			}
			
			@Override
			public int compare(DIFMFile o1, DIFMFile o2)
			{
				double p1 = calPriority(o1);
				double p2 = calPriority(o2);
				return (p1 > p2) ? -1 : ((p1 < p2) ? 1 : o1.getName().compareTo(o2.getName()));
			}
			
		}));
		
		
		peers = new ConcurrentLinkedQueue<>();
		peerURIs = Collections.synchronizedList(new LinkedList<String>());
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
		switch (t)
		{
			case PEER_CONNECTED:
			{
				String uri = (String) msg;
				PeerInterface newPeer = DIUtils.getPeerFromURI(uri);
				peers.add(newPeer);
				peerIsConnected(newPeer);
				return null;
			}
			case PEER_LIST:
			{
				String[] uris = (String[])msg;
				PeerInterface peer;
				for(String uri : uris)
				{
					peer = DIUtils.getPeerFromURI(uri);
					peers.add(peer);
					peerIsConnected(peer);
				}
				peer = DIUtils.getPeerFromURI(fromURI);
				peers.add(peer);
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
				Map<String, Object>[] fileInfos = (Map<String, Object>[])msg;
				for(Map<String, Object> fileInfo : fileInfos)
				{
					String name = (String)fileInfo.get("name");
					System.out.println("File info of "+name+" is received.");
					DIFMFile file = new DIFMFile(name, 
							(long)fileInfo.get("length"), 
							(double)fileInfo.get("priority"), 
							wantingPeers.get(name).size());
					fileMap.put(name, file);
					fileInfoReceived(file);
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
	 * @param file 
	 */
	private void fileInfoReceived(DIFMFile file)
	{
		if(wantingPeers.get(file.getName()).contains(thisURI))
		{
			fileQueue.add(file);
		}
	}
	
	private Object[] broadcast(final MsgType t, final Object msg, boolean wait)
	{
		if(wait)
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
		Object[] res = new Object[peers.size()];
		int i = 0;
		for(PeerInterface p : peers)
		{
			res[i] = p.processMsg(t, msg, thisURI);
			i++;
		}
		return res;
	}
	
	private void peerIsConnected(PeerInterface peer)
	{
		System.out.println("Peer "+peer.toString()+" is connected.");
		Thread t = new Thread(this, "Download thread from "+peer.toString());
		downloadingThreads.add(t);
		t.start();
	}
	
	@Override
	public AtomicBitSet getExistingPcs(String file)
	{
		return fileMap.get(file).getExisting();
	}
	
	private Piece getPieceToDownload(PeerInterface target)
	{
		
		//Select a file to download based on priority
		DIFMFile file = DIUtils.getFileProportionally(fileQueue, 5);
		Piece p = null;
		if(file != null)
		{
			//Get existing pieces and interesting pieces from remote peer
			AtomicBitSet remoteExisting = target.getExistingPcs(file.getName());
			file.incrementPieceSeen(remoteExisting);
			AtomicBitSet interesting = (AtomicBitSet)remoteExisting.clone();
			interesting.andNot(file.getExisting());

			p = file.getPieceToDownload(interesting);
		}
		return p;
	}
	
	/**
	 * Finalize piece information after the piece is downloaded
	 * @param p 
	 */
	private void pieceIsDownloaded(Piece p)
	{
		DIFMFile file = fileMap.get(p.name);
		
		//Update file priority
		fileQueue.remove(file);
		fileQueue.add(file);
				
		if(file.isCompleted())
		{
			fileIsDownloaded(file);
		}
		transferredBytes += p.length;
	}
	
	public void workerConnected(String uri)
	{
		synchronized(peers)
		{
			broadcast(MsgType.PEER_CONNECTED, uri, true);
			PeerInterface newPeer = DIUtils.getPeerFromURI(uri);
			String[] peerList = new String[peerURIs.size()];
			newPeer.processMsg(MsgType.PEER_LIST, peerURIs.toArray(peerList), thisURI);
			peers.add(newPeer);
			peerIsConnected(newPeer);
		}
	}
	
	/**
	 * Called when the file is completely downloaded
	 * @param name 
	 */
	private void fileIsDownloaded(DIFMFile file)
	{
		file.finallized();
		fileQueue.remove(file);
		
		Object lock = waitingLocks.get(file.getName());
		if(lock != null)
		{
			synchronized(lock)
			{
				lock.notifyAll();
			}
		}
	}
	
	/**
	 * Inactivate the file
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
		_inactivateFile(fileMap.get(filename));
		broadcast(MsgType.FILE_INACTIVATE, filename, false);
	}
	
	/**
	 * called when new file is created
	 * @param name
	 * @param length
	 * @param priority 
	 */
	private void fileCreated(String name, long length, double priority)
	{
		Set<String> peersWantThisFile = wantingPeers.get(name);
		boolean requiredByThisPeer = peersWantThisFile.contains(thisURI);
		
		
		DIFMFile file = new DIFMFile(name, length, priority, peersWantThisFile.size());
		file.setIfRequired(requiredByThisPeer);
		fileMap.put(name, file);
		file.setComplete();
		fileIsDownloaded(file);
		
	}
	
	private void download()
	{
		PeerInterface target;
		synchronized(peers)
		{
			target = peers.poll();
			peers.add(target);
		}
		
		Piece p = getPieceToDownload(target);
		
		if(p != null)
		{
			System.out.println("Downloading "+p.name+"("+p.index+") from "
					+target.toString()+"...");
			byte[] content = target.getPieceContent(p.name, p.index);
			fileMap.get(p.name).write(p.index, content, p.length);
			pieceIsDownloaded(p);
			System.out.println("Done.");
		}
		System.gc();
	}
	
	/**
	 * Get the content of the piece to downloading peer
	 * @param name
	 * @param index
	 * @return 
	 */
	@Override
	public byte[] getPieceContent(String name, int index)
	{
		System.out.println("Sending "+name+"("+index+")");
		DIFMFile file = fileMap.get(name);
		byte[] buffer = file.read(index, file.getPiece(index).length);
		file.getPiece(index).seen();
		return buffer;
	}
	
	/**
	 * Store information of schedule
	 * @param s 
	 */
	public void setSchedule(Schedule s)
	{
		Set<String> tasks = s.getSettings().getTaskUUIDSet();
		String wfid = s.getWorkflowID();
		for(String tid : tasks)
		{
			Task t = Task.get(tid);
			String worker = s.getWorkerForTask(tid);
			
			//Add wantingFiles and wantingPeers
			for(String fid : t.getInputFiles())
			{
				_setFilePeerRequirement(WorkflowFile.get(fid).getName(wfid), worker);
			}
		}
		
		//Set to transfer workflow's output files to this peer
		for(String fid: s.getSettings().getWorkflow().getOutputFiles())
		{
			_setFilePeerRequirement(WorkflowFile.get(fid).getName(wfid), thisURI);
		}
		
		//Broadcast file requirement information
		broadcast(MsgType.FILE_REQ_INFO, new Map[]{wantingFiles, wantingPeers}, true);
		
		//Notify the file manager that the files are created
		outputFilesCreated(wfid, s.getSettings().getWorkflow().getInputFiles());
	}
	
	private void _setFilePeerRequirement(String filename, String peerURI)
	{
		Set<String> wantingFilesOfWorker = wantingFiles.get(peerURI);
		if(wantingFilesOfWorker == null)
		{
			wantingFilesOfWorker = Collections.synchronizedSet(new HashSet<String>());
			wantingFiles.put(peerURI, wantingFilesOfWorker);
		}
		wantingFilesOfWorker.add(filename);

		Set<String> wantingWorkersOfFile = wantingPeers.get(filename);
		if(wantingWorkersOfFile == null)
		{
			wantingWorkersOfFile = Collections.synchronizedSet(new HashSet<String>());
			wantingPeers.put(filename, wantingWorkersOfFile);
		}
		wantingWorkersOfFile.add(peerURI);
	}
	
	/**
	 * Block until the file is completely downloaded
	 * @param name 
	 */
	@Override
	public void waitForFile(String name)
	{
		System.out.println("Waiting for "+name);
		Object lock = waitingLocks.get(name);
		if(lock == null)
		{
			lock = name;
			waitingLocks.put(name, name);
		}
		synchronized(lock)
		{
			DIFMFile file = fileMap.get(name);
			if (file == null)
			{
				while (!Utils.fileExists(Utils.getProp("working_dir") + "/" + name))
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
			else
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
		waitingLocks.remove(name);
	}
	
	/**
	 * Called when new file is created
	 * @param fname 
	 */
	@Override	
	public void outputFilesCreated(String wfid, Set<String> files)
	{
		LinkedList<Map<String, Object>> fileToBroadcast = new LinkedList<>();
		for(String fid : files)
		{
			WorkflowFile f = WorkflowFile.get(fid);
			String fname = f.getName(wfid);
			long length = Utils.getFileLength(Utils.getProp("working_dir") + "/" + fname);
			fileCreated(fname, length, f.getPriority());
			
			Set<String> peersWantingThisFile = wantingPeers.get(fname);
			if(peersWantingThisFile != null && 
					!(peersWantingThisFile.size() == 1 && peersWantingThisFile.contains(thisURI)))
			{
				HashMap<String, Object> fileInfo = new HashMap<>();
				fileInfo.put("name", fname);
				fileInfo.put("length", length);
				fileInfo.put("priority", f.getPriority());
				fileToBroadcast.add(fileInfo);
			}
		}
		
		if(!fileToBroadcast.isEmpty())	
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
		while(!stopping)
		{
			//Wait until a file is available
			while(fileQueue.isEmpty())
			{
				synchronized(this)
				{
					try
					{
						this.wait(5000);
					}
					catch (InterruptedException ex)
					{}
				}
			}
			try
			{
				this.download();
			}
			catch(OutOfMemoryError e)
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
	
	

}
