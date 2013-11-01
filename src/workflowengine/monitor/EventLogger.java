/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.monitor;

import java.io.Serializable;
import java.text.DateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.CategoryToolTipGenerator;
import org.jfree.chart.labels.IntervalCategoryToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.data.category.CategoryDataset;
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
	private final Map<String, List<Event>> events = new ConcurrentHashMap<>();
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
	
	public void remove(Event e)
	{
		events.get(e.name).remove(e);
	}
	
	/**
	 * 
	 * @param name
	 * @return event ID
	 */
	public Event start(String name, String desc)
	{		
		long start = Utils.time();
		maxStart = Math.max(start, maxStart);
		Event e = new Event(name, desc, start);
		List<Event> list = events.get(name);
		if(list == null)
		{
			list = new LinkedList<>();
			events.put(name, list);
		}
		list.add(e);
		synchronized (eventList)
		{
			eventList.add(e);
		}
		return e;
	}
	
	public void finish(String name)
	{
		long fin = Utils.time();
		maxFinish = Math.max(fin, maxFinish);
		getFirstUnfinishedEvent(name).end = fin;
	}
	
	private Event getFirstUnfinishedEvent(String name)
	{
		List<Event> list = events.get(name);
		if(list == null)
		{
			return null;
		}
		for(Event e : list)
		{
			if(e.end == -1)
			{
				return e;
			}
		}
		return null;
	}

	public void event(String name, String desc)
	{
		start(name, desc);
		finish(name);
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
		Random r = new Random();
		long end = (Math.max(maxStart, maxFinish))+60;
		
		TaskSeries s1 = new TaskSeries("Events");
		
		for(Event e : eventList)
		{
			Task t = new Task(e.name+' '+(char)r.nextInt(),new SimpleTimePeriod(
					e.start * 1000,
					e.end == -1 ? end*1000 : e.end*1000 + 50
					));
			s1.add(t);
		}
		TaskSeriesCollection collection = new TaskSeriesCollection();
        collection.add(s1);
		
		JFreeChart chart = ChartFactory.createGanttChart(
            "Event Chart",
            "", "", collection, false, true, false
        );
		((CategoryPlot)chart.getPlot()).getRenderer().setBaseToolTipGenerator(
				new IntervalCategoryToolTipGenerator(
					"{3} - {4}", 
					DateFormat.getDateTimeInstance(
						DateFormat.SHORT, DateFormat.SHORT)));
		return chart;
	}
	
	public int size()
	{
		return eventList.size();
	}
}

