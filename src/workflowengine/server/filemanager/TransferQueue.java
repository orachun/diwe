/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.util.Collection;
import java.util.HashMap;
import java.util.TreeSet;

/**
 *
 * @author orachun
 */
public class TransferQueue
{
	private HashMap<PieceInfo, Entry> map = new HashMap<>();
	private static class Entry implements Comparable<Entry>
	{
		public final PieceInfo p;
		public double priority;

		public Entry(PieceInfo p, double priority)
		{
			this.p = p;
			this.priority = priority;
		}

		@Override
		public int compareTo(Entry o)
		{
			if(priority > o.priority)
			{
				return -1;
			}
			if(priority < o.priority)
			{
				return 1;
			}
			return 0;
		}
	}
	private TreeSet<Entry> queue = new TreeSet<>();
	public void add(PieceInfo p, int requiringSites)
	{
		Entry e = map.get(p);
		if(e == null)
		{
			e = new Entry(p, p.priority*(requiringSites+1));
			map.put(p, e);
		}
		else
		{
			e.priority = p.priority*requiringSites;
			queue.remove(e);
		}
		queue.add(e);
	}
	
	public PieceInfo poll()
	{
		Entry e = queue.pollFirst();
		if(e == null)
		{
			return null;
		}
		map.remove(e.p);
		return e.p;
	}
	
	public void removeAll(Collection<PieceInfo> pieces)
	{
		for(PieceInfo p : pieces)
		{
			Entry e = map.remove(p);
			if(e != null)
			{
				queue.remove(e);
			}
		}
	}
}
