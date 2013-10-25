/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.monitor;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.data.gantt.Task;
import org.jfree.data.gantt.TaskSeries;
import org.jfree.data.gantt.TaskSeriesCollection;
import org.jfree.data.time.SimpleTimePeriod;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class EventLogger implements Serializable
{
	private final Map<String, Event> events = new ConcurrentHashMap<>();
	private final LinkedList<Event> eventList = new LinkedList<>();
	private long maxStart = -1;
	private long maxFinish = -1;
	public static class Event implements Serializable
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

		
		
		
	}
	
	/**
	 * 
	 * @param name
	 * @return event ID
	 */
	public void start(String name, String desc)
	{
		if (events.containsKey(name))
		{
			throw new IllegalArgumentException("Event name already exists");
		}
		
		long start = Utils.time();
		maxStart = Math.max(start, maxStart);
		Event e = new Event(name, desc, start);

		events.put(name, e);
		synchronized (eventList)
		{
			eventList.add(e);
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
		
		long start = eventList.getFirst().start;
		long end = eventList.getLast().end == -1 ? Utils.time() : eventList.getLast().end;
		
		StringBuilder sb = new StringBuilder();
		sb.append("<h1>Event Log</h1>");
		sb.append("<div>From ")
				.append(df.format(new Date(start*1000)))
				.append(" to ")
				.append(df.format(new Date(end*1000)))
				.append(" (").append(end-start).append("s)");
		for(Event e : eventList)
		{
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
	
	public JFreeChart toGanttChart()
	{
		long end = (Math.max(maxStart, maxFinish))+60;
		
		TaskSeries s1 = new TaskSeries("Events");
		
		for(Event e : eventList)
		{
			s1.add(new Task(e.name,new SimpleTimePeriod(
					e.start * 1000,
					e.end == -1 ? end*1000 : e.end*1000
					)));
		}
		TaskSeriesCollection collection = new TaskSeriesCollection();
        collection.add(s1);
		
		JFreeChart chart = ChartFactory.createGanttChart(
            "Event Chart",
            "", "", collection
        );
		return chart;
	}
	
	public int size()
	{
		return eventList.size();
	}
}

