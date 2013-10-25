/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.SchedulingSettings;

/**
 *
 * @author orachun
 */
public class ABC implements Scheduler
{
	private SchedulingSettings ss;
	private String[] workers;
	private String[] tasks;
	private Solution[] swarm;
	private Solution best;
	private Random r = new Random();
	private static final double ADJUST_PROP = 0.5;
	private static final int SWARM_SIZE = 10;
	private static final int ITERATIONS = 10;
	private static final int NOT_IMPROVE_LIMIT = 5;
	private ExecutorService threadPool = Executors.newFixedThreadPool(5);
	List<Future> futures = new LinkedList<>();
	private class Solution
	{
		private int sol[];
		private Schedule sch;
		private boolean edited = false;
		private int notImproveCount = 0;
		public Solution(int totalTasks)
		{
			sol = new int[totalTasks];
		}
		public synchronized Schedule getSch()
		{
			if(!edited && sch != null)
			{
				return sch;
			}
			if(sch == null)
			{
				sch = new Schedule(ss);
			}
			for(int i=0;i<tasks.length;i++)
			{
				sch.setWorkerForTask(tasks[i], workers[sol[i]]);
			}
			edited = false;
			return sch;
		}
		
		public synchronized double getFitness()
		{
			if(best == null || ss.getFc().isScheduleBetter(this.getSch(), best.getSch()))
			{
				best = this.copy();
			}
			return this.getSch().getFitness();
		}
		
		public synchronized void random()
		{
			for(int i=0;i<sol.length;i++)
			{
				sol[i] = r.nextInt(workers.length);
			}
			edited = true;
			notImproveCount = 0;
		}
		
		public synchronized void adjust()
		{
			Solution old = this.copy();
			while(!edited)
			{
				for(int i=0;i<sol.length;i++)
				{
					if(r.nextDouble() < ADJUST_PROP)
					{
						sol[i] = r.nextInt(workers.length);
						edited = true;
					}
				}
			}
			if(ss.getFc().isScheduleBetter(old.getSch(), this.getSch()))
			{
				sol = old.sol;
				sch = old.sch;
				edited = false;
				notImproveCount++;
			}
			else
			{
				notImproveCount = 0;
			}
		}
		
		public synchronized void adjust(Solution s)
		{
			Solution old = this.copy();
			while(!edited)
			{
				for(int i=0;i<sol.length;i++)
				{
					if(r.nextDouble() < ADJUST_PROP)
					{
						sol[i] = s.sol[i];
						edited = true;
					}
				}
			}
			if(ss.getFc().isScheduleBetter(old.getSch(), this.getSch()))
			{
				sol = old.sol;
				sch = old.sch;
				edited = false;
				notImproveCount++;
			}
			else
			{
				notImproveCount = 0;
			}
		}
		
		public synchronized void scout()
		{
			if(notImproveCount >= NOT_IMPROVE_LIMIT)
			{
				random();
			}
		}
		
		public synchronized Solution copy()
		{
			Solution s = new Solution(tasks.length);
			s.sch = sch.copy();
			s.edited = edited;
			return s;
		}
	}

	private void barrier()
	{
		for(Future f : futures)
		{
			try
			{
				f.get();
			}
			catch (InterruptedException | ExecutionException ex)
			{}
		}
		futures.clear();
	}
	
	@Override
	public Schedule getSchedule(SchedulingSettings settings)
	{
		ss = settings;
		workers = settings.getSiteArray();
		tasks = settings.getTaskArray();
		swarm = new Solution[SWARM_SIZE];
		for(int i=0;i<SWARM_SIZE;i++)
		{
			final int index = i;
			futures.add(threadPool.submit(new Runnable() {
				@Override
				public void run()
				{
					Solution s = new Solution(tasks.length);
					s.random();
					synchronized(swarm)
					{
						swarm[index] = s;
					}
				}
			}));
		}
		barrier();
		for(int loop = 0;loop<ITERATIONS;loop++)
		{
			for(final Solution s : swarm)
			{
				futures.add(threadPool.submit(new Runnable() {
					@Override
					public void run()
					{
						s.adjust();
					}
				}));
			}
			barrier();
			for(int i=0;i<SWARM_SIZE;i++)
			{
				futures.add(threadPool.submit(new Runnable() {
					@Override
					public void run()
					{
						select().adjust();
					}
				}));
			}
			barrier();
			for(final Solution s : swarm)
			{
				futures.add(threadPool.submit(new Runnable() {
					@Override
					public void run()
					{
						s.scout();
					}
				}));
			}
			barrier();
		}
		return best.getSch();
	}
	
	private Solution select()
	{
		double total = 0;
		double sum = 0;
		double rand = r.nextDouble();
		Solution selected = null;
		for(Solution s : swarm)
		{
			total += 1/s.getFitness();
		}
		for(Solution s : swarm)
		{
			sum += 1/s.getFitness();
			if(sum/total > rand)
			{
				selected = s;
				break;
			}
		}
		return selected;
	}
}
