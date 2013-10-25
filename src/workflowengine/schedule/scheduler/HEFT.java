/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import java.util.Arrays;
import workflowengine.schedule.SchedulingSettings;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import workflowengine.schedule.Schedule;
import workflowengine.workflow.Task;

/**
 *
 * @author udomo
 */
public class HEFT implements Scheduler
{
    SchedulingSettings settings;
    double[] avgExecTime;
    double[][] avgCommTime;
    double[] rank;
	String[] tasks;
	String[] sites;

    @Override
    public Schedule getSchedule(SchedulingSettings settings)
    {
        this.settings = settings;
		tasks = settings.getTaskArray();
		sites = settings.getSiteArray();
        avgExecTime = new double[tasks.length];
        avgCommTime = new double[tasks.length][tasks.length];
        rank = new double[tasks.length];
        
        calAvgExec();
        calAvgComm();
        for (int i = 0; i < tasks.length; i++)
        {
            rank[i] = -1;
        }
        for(String t : tasks)
        {
            rank(t);
        }
        LinkedList<String> sortedTask = getSortedTasks();
        
        return calSchedule(sortedTask);
    }
    
    Schedule calSchedule(LinkedList<String> sortedTasks)
    {
        Schedule s = new Schedule(settings);
        HashMap<String, Double> workerReadyTime = new HashMap<>(sites.length);
        HashMap<String, Double> taskFinishTime = new HashMap<>(sites.length);
        for(String w : sites)
        {
            workerReadyTime.put(w, 0.0);
        }
        while (!sortedTasks.isEmpty())
        {
            String t = sortedTasks.poll();
            
            if(!taskFinishTime.keySet().containsAll(settings.getWorkflow().getParent(t)))
            {
                sortedTasks.add(t);
                continue;
            }
            
            String targetW = null;
            double parentFinTime = Double.NEGATIVE_INFINITY;
            
            
            double minFinTime = Double.POSITIVE_INFINITY;
            for(String w : sites)
            {
                for(String p : settings.getWorkflow().getParent(t))
                {
                    String parentWorker = s.getWorkerForTask(p);
					Task parentTask = Task.get(p);
                    double commTime = parentWorker.equals(w) ? 0 : 
							settings.getExecNetwork()
								.getTransferTime(parentWorker, w, 
								parentTask.getOutputFileUUIDsForTask(t));
                    parentFinTime = Math.max(parentFinTime, taskFinishTime.get(p)+commTime);
                }
				Task task = Task.get(t);
                double finTime = Math.max(parentFinTime, workerReadyTime.get(w))+task.getEstimatedExecTime();
                if(finTime < minFinTime)
                {
                    minFinTime = finTime;
                    targetW = w;
                }
            }
            s.setWorkerForTask(t, targetW);
            workerReadyTime.put(targetW, minFinTime);
            taskFinishTime.put(t, minFinTime);
        }
        return s;
    }

    void calAvgExec()
    {
        for (int i=0;i<tasks.length;i++)
        {
            double sum = 0;
			Task task = Task.get(tasks[i]);
            avgExecTime[i] = task.getEstimatedExecTime();
        }
    }
    
    void calAvgComm()
    {
        for (int t = 0; t < tasks.length; t++)
        {
            String parent = tasks[t];
            Set<String> children = settings.getWorkflow().getChild(parent);
            for (String child: children)
            {
				Set<String> files = Task.get(parent).getOutputFileUUIDsForTask(child);
                double sum = 0;
                for (String s1 : sites)
                {
                    for (String s2 : sites)
                    {
                        sum = sum + settings.getExecNetwork().getTransferTime(s1, s2, files);
                    }
                }
                avgCommTime[t][taskIndex(child)] = sum / sites.length / sites.length;
            }
        }
    }
    
    double rank(String t)
    {
        int ti = taskIndex(t);
        if (rank[ti] == -1)
        {
            double r = avgExecTime[ti];
            double max = 0;
            for (String c : settings.getWorkflow().getChild(t))
            {
                int tc = taskIndex(c);
                max = Math.max(max, avgCommTime[ti][tc] + rank(c));
            }
            r = r + max;
            rank[ti] = r;
            return r;
        }
        else
        {
            return rank[ti];
        }
    }
    
    LinkedList<String> getSortedTasks()
    {
        LinkedList<String> sortedTasks = new LinkedList<>(Arrays.asList(tasks));
        Collections.sort(sortedTasks, new Comparator<String>() {
            @Override
            public int compare(String t1, String t2)
            {
                double r1 = rank[taskIndex(t1)];
                double r2 = rank[taskIndex(t2)];
                if(r1 < r2)
                {
                    return 1;
                }
                if(r1 == r2)
                {
                    return 0;
                }
                return -1;
            }
        });
        return sortedTasks;
    }
    
	
	private int taskIndex(String t)
	{
		for(int i=0;i<tasks.length;i++)
		{
			if(t.equals(tasks[i]))
			{
				return i;
			}
		}
		return -1;
	}
}


