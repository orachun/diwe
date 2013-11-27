/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;

/**
 *
 * @author orachun
 */
public class DIUtils
{
	private static Random r = new Random();
	static <T extends Object> T getElementProportionally(SortedSet<T> set, int candidateSize)
	{
		
		ArrayList<T> candidates = new ArrayList<>(candidateSize);
		Iterator<T> iterator = set.iterator();
		for(int i=0;i<candidateSize && iterator.hasNext();i++)
		{
			T f = iterator.next();
			candidates.add(f);
		}
		if(!candidates.isEmpty())
		{
			return candidates.get(r.nextInt(candidates.size()));
		}
		return null;
	}
	
	static PeerInterface getPeerFromURI(String uri)
	{
		throw new IllegalStateException("not implemented");
	}
	
	static void registerLocalPeer(String uri, PeerInterface peer)
	{
		
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
			if(o1.getSeen() < o2.getSeen())
			{
				return -1;
			}
			if(o1.getSeen() > o2.getSeen())
			{
				return 1;
			}
			return (Math.random() > 0.5) ? -1 : 1;
		}
	};
	
	static Comparator<Piece> getPieceComparator()
	{
		return pieceComparator;
	}
}
