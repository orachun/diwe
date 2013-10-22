/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.monitor;

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class EventLogger
{
	private final Map<String, Event> events = new HashMap<>();
	private final TreeSet<Event> eventSet = new TreeSet<>();
	private long maxStart = -1;
	private long maxFinish = -1;
	public static class Event implements Comparable<Event>
	{ 
		String name;
		String desc;
		long start;
		long end;
		public Event(String name, String desc, long start)
		{
			this.name = name;
			this.start = start;
			this.end = -1;
		}

		long duration()
		{
			if(end == -1)
			{
				return Utils.time()-start;
			}
			return end - start;
		}

		public String getName()
		{
			return name;
		}

		public long getStart()
		{
			return start;
		}

		public long getEnd()
		{
			return end;
		}

		@Override
		public int compareTo(Event o)
		{
			return start < o.start? -1 : start == o.start ? 0 : 1;
		}
		
		
		
	}
	
	/**
	 * 
	 * @param name
	 * @return event ID
	 */
	public void start(String name, String desc)
	{
		synchronized (events)
		{
			if (events.containsKey(name))
			{
				throw new IllegalArgumentException("Event name already exists");
			}
		}
		long start = Utils.time();
		maxStart = Math.max(start, maxStart);
		Event e = new Event(name, desc, start);

		synchronized (events)
		{
			events.put(name, e);
			eventSet.add(e);
		}
	}
	
	public void finish(String name)
	{
		long fin = Utils.time();
		maxFinish = Math.max(fin, maxFinish);
		
		synchronized(events)
		{
			events.get(name).end = fin;
		}
	}

	public long getMaxStart()
	{
		return maxStart;
	}

	public long getMaxFinish()
	{
		return maxFinish;
	}
	
	public String toHTML()
	{
		DateFormat df = DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.MEDIUM);
		
		long start = eventSet.first().start;
		long end = eventSet.last().end == -1 ? Utils.time() : eventSet.last().end;
		
		StringBuilder sb = new StringBuilder();
		Iterator<Event> it = eventSet.iterator();
		sb.append("<h1>Event Log</h1>");
		sb.append("<div>From ")
				.append(df.format(new Date(start*1000)))
				.append(" to ")
				.append(df.format(new Date(end*1000)))
				.append(" (").append(end-start).append("s)");
		while(it.hasNext())
		{
			Event e = it.next();
			sb.append("<div class=\"event-item\">[")
					.append(df.format(new Date(e.start*1000)))
					.append("-")
					.append(e.end == -1 ? "X" : df.format(new Date(e.end*1000)))
					.append("(")
					.append(e.duration())
					.append(")")
					.append("] ")
					.append(e.name)
					.append("</div>");
		}
		return sb.toString();
	}
}

