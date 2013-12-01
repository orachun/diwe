/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import lipermi.net.Client;
import workflowengine.utils.Utils;

import static workflowengine.server.filemanager.difm.NewDIFM.END_GAME_MODE_RATIO;

/**
 *
 * @author orachun
 */
public class DIUtils
{
	private static int PORT_SHIFT = 100;
	private static Random r = new Random();
	static DIFMFile getFileProportionally(SortedSet<DIFMFile> set, int candidateSize)
	{
		ArrayList<DIFMFile> candidates = new ArrayList<>(candidateSize);
		Iterator<DIFMFile> iterator = set.iterator();
		try
		{
			while(candidates.size() < candidateSize && iterator.hasNext())
			{
				DIFMFile f = iterator.next();
				if(!f.isCompleted() && f.getPercentDownloaded() > END_GAME_MODE_RATIO)
				{
					return f;
				}
				candidates.add(f);
			}
			if(!candidates.isEmpty())
			{
				return candidates.get(r.nextInt(candidates.size()));
			}
		}
		catch(ConcurrentModificationException e)
		{
			if(!candidates.isEmpty())
			{
				return candidates.get(r.nextInt(candidates.size()));
			}
		}
		return null;
	}
	
	static Piece getPieceProportionally(SortedSet<Piece> set, int candidateSize, AtomicBitSet interesting)
	{
		
		ArrayList<Piece> candidates = new ArrayList<>(candidateSize);
		Iterator<Piece> iterator = set.iterator();
		while(candidates.size() < candidateSize && iterator.hasNext())
		{
			Piece p = iterator.next();
			if(interesting.get(p.index))
			{
				candidates.add(p);
			}
		}
		if(!candidates.isEmpty())
		{
			return candidates.get(r.nextInt(candidates.size()));
		}
		return null;
	}
	
	static PeerInterface getPeerFromURI(String uri)
	{
		PeerInterface peer = null;
		int tries = 0;
		String[] s = uri.split(":");
		String realURI = s[0] + ":" + (PORT_SHIFT + Integer.parseInt(s[1]));
		while (peer == null && tries < 10)
		{
			try
			{
				Client c = Utils.getRMIClient(realURI);
				peer = (PeerInterface) c.getGlobal(PeerInterface.class);
			}
			catch (Exception e)
			{
				peer = null;
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
		return peer;
	}
	
	static void registerLocalPeer(String uri, PeerInterface peer)
	{
		Utils.registerRMIServer(PeerInterface.class, peer, 
				PORT_SHIFT + Utils.getIntProp("local_port"));
	}
	
	static void mergeFileMap(Map<String, Set<String>> from, Map<String, Set<String>> to)
	{
		for(Map.Entry<String, Set<String>> entry : from.entrySet())
		{
			Set<String> targetSet = to.get(entry.getKey());
			if(targetSet == null)
			{
				to.put(entry.getKey(), entry.getValue());
			}
			else
			{
				targetSet.addAll(entry.getValue());
			}
		}
	}
	
	static void notifyDownloadThread(List<Thread> threads)
	{
		for(Thread t : threads)
		{
			synchronized(t)
			{
				t.notifyAll();
			}
		}
	}
	
	
	private static Comparator<Piece> pieceComparator = new Comparator<Piece>()
	{
		@Override
		public int compare(Piece o1, Piece o2)
		{
			if(o1.equals(o2))
			{
				return 0;
			}
			if(o1.getSeen() < o2.getSeen())
			{
				return -1;
			}
			if(o1.getSeen() > o2.getSeen())
			{
				return 1;
			}
			//return Math.random() > 0.5 ? -1 : 1;
			return o1.toString().compareTo(o2.toString());
		}
	};
	
	public static Comparator<Piece> getPieceComparator()
	{
		return pieceComparator;
	}
}
