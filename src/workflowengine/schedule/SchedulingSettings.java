/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import java.util.Collections;
import workflowengine.schedule.fc.FC;
import workflowengine.schedule.fc.MakespanFC;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import workflowengine.server.WorkflowExecutor;
import workflowengine.resource.ExecutorNetwork;
import workflowengine.workflow.Workflow;

/**
 *
 * @author udomo
 */
public class SchedulingSettings
{
	private WorkflowExecutor we;
    private Workflow wf;
    private Set<String> taskSet;
    private HashMap<String, String> fixedMapping; //For finished tasks (TaskUUID, WorkerURI
    private Set<String> fixedTasks;
    private HashMap<String, Object> params = new HashMap<>();
    private FC fc;
	private ExecutorNetwork execNetwork;
//	private Map<String, Site> siteMap = new HashMap<>();
	
    public SchedulingSettings(WorkflowExecutor we, Workflow wf, ExecutorNetwork execNetwork)
	{
		this(we, wf, execNetwork, null, null);
	}
	public SchedulingSettings(WorkflowExecutor we, Workflow wf, ExecutorNetwork execNetwork, FC fc)
	{
		this(we, wf, execNetwork, fc, null);
	}
	public SchedulingSettings(WorkflowExecutor we, Workflow wf, ExecutorNetwork execNetwork, HashMap<String, String> fixedMapping)
	{
		this(we, wf, execNetwork, null, fixedMapping);
	}
	public SchedulingSettings(WorkflowExecutor we, Workflow wf, ExecutorNetwork execNetwork, FC fc, HashMap<String, String> fixedMapping)
	{
		if(fc == null)
        {
            this.fc = new MakespanFC();
        }
        else
        {
            this.fc = fc;
        }
		this.we = we;
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
		
//		for(String uri : we.getWorkerSet())
//		{
//			siteMap.put(uri, new Site(uri, we.getWorker(uri).getTotalProcessors()));
//		}
//		siteMap = Collections.unmodifiableMap(siteMap);
		
	}
    public FC getFc()
    {
        return fc;
    }
    
    private Workflow generateWorkflow(Workflow originalWf, HashMap<String, String> fixedMapping)
    {
		Set<String> subTasks = originalWf.getTaskSet();
		subTasks.removeAll(fixedMapping.keySet());
        return originalWf.getSubworkflow(originalWf.getName(), subTasks);
    }
    
    
    public HashMap<String, String> getFixedMapping()
    {
        return fixedMapping;
    }
    
    public boolean isFixedTask(String t)
    {
        return fixedMapping.containsKey(t);
    }
    
    
    public Set<String> getFixedTasks()
    {
        return new HashSet<>(fixedTasks);
    }
    
    public int getTotalTasks()
    {
        return taskSet.size();
    }
    
    public Set<String> getStartTasks()
    {
        return wf.getStartTasks();
    }

    public Set<String> getEndTasks()
    {
        return wf.getEndTasks();
    }

    public Set<String> getTaskUUIDSet()
    {
        return taskSet;
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
    public int getTotalWorkers()
    {
        return execNetwork.getExecutorURISet().size();
    }

	public ExecutorNetwork getExecNetwork()
	{
		return execNetwork;
	}
    
	
	public Map<String, Site> getSiteMap()
	{
		HashMap<String, Site> map = new HashMap<>();
		for(String uri : we.getWorkerSet())
		{
			map.put(uri, new Site(uri, we.getWorker(uri).getTotalProcessors()));
		}
		return map;
	}
	
//	public Site getSite(String uri)
//	{
//		return siteMap.get(uri);
//	}
	
	public String[] getTaskArray()
	{
		return this.taskSet.toArray(new String[this.getTotalTasks()]);
	}
	public String[] getSiteArray()
	{
		return this.execNetwork.getExecutorURISet()
				.toArray(new String[this.getTotalWorkers()]);
	}
	
	public String getAnyWorker()
	{
		return this.execNetwork.getExecutorURISet().iterator().next();
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
    
	public WorkflowExecutor getExecutor()
	{
		return we;
	}
	
}
