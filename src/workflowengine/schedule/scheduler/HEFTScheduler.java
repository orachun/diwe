/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule.scheduler;

import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.scheduler.Scheduler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import removed.Worker;
import workflowengine.schedule.Schedule;
import workflowengine.workflow.Task;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author udomo
 */
public class HEFTScheduler implements Scheduler
{

    SchedulingSettings settings;
    int totalTasks;
    int totalWorkers;
    double[] avgExecTime;
    double[][] avgCommTime;
    double[] rank;

    @Override
    public Schedule getSchedule(SchedulingSettings settings)
    {
        this.settings = settings;
        totalTasks = settings.getTotalTasks();
        totalWorkers = settings.getTotalWorkers();
        avgExecTime = new double[totalTasks];
        avgCommTime = new double[totalTasks][totalTasks];
        rank = new double[totalTasks];
        
        calAvgExec();
        calAvgComm();
        for (int i = 0; i < totalTasks; i++)
        {
            rank[i] = -1;
        }
        for(Task t : settings.getStartTasks())
        {
            rank(t);
        }
        LinkedList<Task> sortedTasks = getSortedTasks();
        
        return calSchedule(sortedTasks);
    }
    
    Schedule calSchedule(LinkedList<Task> sortedTasks)
    {
        Schedule s = new Schedule(settings);
        HashMap<Worker, Double> workerReadyTime = new HashMap<>(totalWorkers);
        HashMap<Task, Double> taskFinishTime = new HashMap<>(totalWorkers);
        for(Worker w : settings.getWorkerIterable())
        {
            workerReadyTime.put(w, 0.0);
        }
        while (!sortedTasks.isEmpty())
        {
            Task t = sortedTasks.poll();
            
            if(!taskFinishTime.keySet().containsAll(settings.getParentTasks(t)))
            {
                sortedTasks.add(t);
                continue;
            }
            
            Worker targetW = settings.getWorker(0);
            double parentFinTime = Double.NEGATIVE_INFINITY;
            
            
            double minFinTime = Double.POSITIVE_INFINITY;
            for(Worker w : settings.getWorkerIterable())
            {
                for(Task p : settings.getParentTasks(t))
                {
                    Worker parentWorker = s.getWorkerForTask(p);
                    double commTime = parentWorker.equals(w) ? 0 : settings.getTransferTime(p.getOutputFileUUIDsForTask(t));
                    parentFinTime = Math.max(parentFinTime, taskFinishTime.get(p)+commTime);
                }
                double finTime = Math.max(parentFinTime, workerReadyTime.get(w))+w.getExecTime(t);
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
        for (int i = 0; i < totalTasks; i++)
        {
            double sum = 0;
            for (int j = 0; j < totalWorkers; j++)
            {
                sum = sum + settings.getWorker(j).getExecTime(settings.getTask(i));
            }
            avgExecTime[i] = sum / totalWorkers;
        }
    }
    
    void calAvgComm()
    {
        for (int t1 = 0; t1 < totalTasks; t1++)
        {
            Task parent = settings.getTask(t1);
            Collection<Task> children = settings.getChildTasks(parent);
            for (Task child : children)
            {
                WorkflowFile[] files = parent.getOutputFileUUIDsForTask(child);
                double sum = 0;
                for (int k = 0; k < totalWorkers; k++)
                {
                    for (int l = 0; l < totalWorkers; l++)
                    {
                        sum = sum + settings.getTransferTime(files);
                    }
                }
                avgCommTime[t1][settings.getTaskIndex(child)] = sum / totalWorkers / totalWorkers;
            }
        }
    }
    
    double rank(Task t)
    {
        int ti = settings.getTaskIndex(t);
        if (rank[ti] == -1)
        {
            double r = avgExecTime[ti];
            double max = 0;
            for (Task c : settings.getChildTasks(t))
            {
                int tc = settings.getTaskIndex(c);
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
    
    LinkedList<Task> getSortedTasks()
    {
        LinkedList<Task> tasks = new LinkedList<>(settings.getTaskList());
//        for(int i=0;i<totalTasks;i++)
//        {
//            tasks.add(settings.getWf().getTask(i));
//        }
        
        Collections.sort(tasks, new Comparator<Task>() {

            @Override
            public int compare(Task o1, Task o2)
            {
                double r1 = rank[settings.getTaskIndex(o1)];
                double r2 = rank[settings.getTaskIndex(o2)];
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
        return tasks;
    }
    
}


