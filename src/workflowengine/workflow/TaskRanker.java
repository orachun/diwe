/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import workflowengine.server.WorkflowExecutor;

/**
 *
 * @author orachun
 */
public class TaskRanker
{
	public static void rankTask(Workflow wf)
	{
		for(String t : wf.getTaskSet())
		{
			Task.get(t).setPriority(-1);
		}
		for(String t : wf.getStartTasks())
		{
			rank(wf, t);
		}
		
		//Calculate priority for each file
		Queue<String> q = wf.getTaskQueue();
		HashMap<String, Double> filePriority = new HashMap<>();
		while(!q.isEmpty())
		{
			Task t = Task.get(q.poll());
			for(String fid : t.getInputFiles())
			{
				Double p = filePriority.get(fid);
				if(p == null)
				{
					p = 0.0;
				}
				p += t.getPriority();
				filePriority.put(fid, p);
			}
		}
		for(Map.Entry<String, Double> entry : filePriority.entrySet())
		{
			WorkflowFile.get(entry.getKey()).setPriority(entry.getValue());
		}
	}
	
	
	/**
	 * rank(t) = exec_time +	   max       (comm_time + rank(c))
	 *                         c=child of t     t to c
	 * @param wf
	 * @param t
	 * @return 
	 */
	
	private static double rank(Workflow wf, String t)
    {
		Task task = Task.get(t);
        if (task.getPriority() == -1)
        {
            double max = 0;
            for (String c : wf.getChild(t))
            {
                double commTime = totalSize(task.getOutputFileUUIDsForTask(c))
						/ WorkflowExecutor.get().getAvgBandwidth();
                max = Math.max(max, commTime + rank(wf, c));
            }
            double r = task.getEstimatedExecTime() + max;
            task.setPriority(r);
            return r;
        }
        else
        {
            return task.getPriority();
        }
    }
	
	private static double totalSize(Set<String> wffs)
	{
		double sum = 0;
		for(String f : wffs)
		{
			sum += WorkflowFile.get(f).getSize();
		}
		return sum;
	}
}
