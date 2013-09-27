/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;
import workflowengine.resource.ExecutorNetwork;
import removed.Worker;
import workflowengine.workflow.Task;
import workflowengine.workflow.Workflow;

/**
 *
 * @author udomo
 */
public class SchedulerSettings
{
    private Random r;
    private Workflow wf;
    private Set<Task> taskSet;
    private HashMap<Task, String> fixedMapping; //For finished tasks
    private Set<Task> fixedTasks;
    private HashMap<String, Object> params = new HashMap<>();
    private FC fc;
	private ExecutorNetwork execNetwork;
	
    public SchedulerSettings(Workflow wf, ExecutorNetwork execNetwork)
	{
		this(wf, execNetwork, null, null);
	}
	public SchedulerSettings(Workflow wf, ExecutorNetwork execNetwork, FC fc)
	{
		this(wf, execNetwork, fc, null);
	}
	public SchedulerSettings(Workflow wf, ExecutorNetwork execNetwork, HashMap<Task, String> fixedMapping)
	{
		this(wf, execNetwork, null, fixedMapping);
	}
	public SchedulerSettings(Workflow wf, ExecutorNetwork execNetwork, FC fc, HashMap<Task, String> fixedMapping)
	{
		if(fc == null)
        {
            this.fc = new MakespanFC();
        }
        else
        {
            this.fc = fc;
        }
        r = new Random();
        this.execNetwork = execNetwork;
        this.fixedMapping = new HashMap<>();
        if(fixedMapping == null)
        {
            fixedTasks = new HashSet<>();
            this.wf = wf;
        }
        else
        {
            this.wf = generateWorkflow(wf, fixedMapping);
            this.fixedMapping.putAll(fixedMapping);
            fixedTasks = fixedMapping.keySet();
        }
        taskSet = this.wf.getTaskSet();
	}
    public FC getFc()
    {
        return fc;
    }
    
    private Workflow generateWorkflow(Workflow originalWf, HashMap<Task, String> fixedMapping)
    {
		Set<Task> subTasks = originalWf.getTaskSet();
		subTasks.removeAll(fixedMapping.keySet());
        return originalWf.getSubworkflow(originalWf.getName(), subTasks);
    }
    
    
    HashMap<Task, String> getFixedMapping()
    {
        return fixedMapping;
    }
    
    public boolean isFixedTask(Task t)
    {
        return fixedMapping.containsKey(t);
    }
    
    
    public Set<Task> getFixedTasks()
    {
        return new HashSet<>(fixedTasks);
    }
    
    public int getTotalTasks()
    {
        return taskSet.size();
    }
    
    public Set<Task> getStartTasks()
    {
        return wf.getStartTasks();
    }

    public Set<Task> getEndTasks()
    {
        return wf.getEndTasks();
    }

    public Set<Task> getTaskSet()
    {
        return taskSet;
    }
    
    public Iterable<Task> getTaskIterable()
    {
        return new Iterable<Task>() {

            @Override
            public Iterator<Task> iterator()
            {
                return taskSet.iterator();
            }
        };
    }
    
    public Workflow getWorkflow()
	{
		return this.wf;
	}
    
    public Iterable<String> getWorkerURIIterable()
    {
        return new Iterable<String>() {

			@Override
			public Iterator<String> iterator()
			{
				return execNetwork.getExecutorURISet().iterator();
			}
		};
    }
    public int getTotalWorkerURIs()
    {
        return execNetwork.getExecutorURISet().size();
    }

	public ExecutorNetwork getExecNetwork()
	{
		return execNetwork;
	}
    
    
    public String getParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : o.toString();
    }
    public char getCharParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : (char)o;
    }
    public Object getObjectParam(String s)
    {
        return params.get(s);
    }
    public double getDoubleParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Double.parseDouble(o.toString());
    }
    public int getIntParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Integer.parseInt(o.toString());
    }
    public boolean getBooleanParam(String s)
    {
        Object o = params.get(s);
        return o == null ? null : Boolean.parseBoolean(o.toString());
    }   
    public void setParam(String s, Object o)
    {
        params.put(s, o);
    }  
    public boolean hasParam(String s)
    {
        return params.containsKey(s);
    }
    
}
