/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import removed.TaskManager;
import workflowengine.utils.DBException;
import workflowengine.utils.Utils;
import workflowengine.utils.simplegraph.DirectedGraph;

/**
 *
 * @author Orachun
 */
public class Workflow implements Serializable
{
    public static final char STATUS_SUBMITTED = 'W';
    public static final char STATUS_SCHEDULED = 'S';
    public static final char STATUS_COMPLETED = 'C';
    
    public static final double AVG_WORKLOAD = 10;
    public static final double AVG_FILE_SIZE = 3 * Utils.MB;
    
    protected String uuid = Utils.uuid();
    protected char status = STATUS_SUBMITTED;
    protected DirectedGraph<Task> taskGraph = new DirectedGraph<>();
    protected String name = "";
    protected Set<WorkflowFile> inputFiles = new HashSet<>();
    protected Set<WorkflowFile> outputFiles = new HashSet<>();
    protected boolean insertToDB;

	protected long startTime = -1;
	protected long scheduledTime = -1;
	protected long finishedTime = -1;
	protected long estimatedFinishedTime = -1;
	protected boolean isFinished = false;
	protected long cumulatedEstimatedExecTime = -1;
	
    public Workflow(String name) throws DBException
    {
        this.name = name;
    }
    
    public void createDummyInputFiles(String destDir)
    {
        for(WorkflowFile f : this.inputFiles)
        {
            String outfile = destDir + f.getName();
            File file = new File(outfile);
            file.getParentFile().mkdirs();
            try
            {
                file.createNewFile();
                Process p = Runtime.getRuntime().exec(new String[]
                {
                    "/bin/bash", "-c", "fallocate -l " + ((int) Math.round(f.getSize())) + "M "+outfile
                });
                p.waitFor();
            }
            catch(IOException | InterruptedException ex)
            {
                TaskManager.logger.log(ex.getMessage());
                TaskManager.logger.log("Exception while creating a dummy input file: "+ex.getMessage());
            }
        }
    }
    
    
	/**
	 * Prepare workflow to be used further.
	 */
	public void finalizeWorkflow()
	{
		new File(Utils.getProp("working_dir")+uuid).mkdir();
		
		inputFiles = new HashSet<>();
		outputFiles = new HashSet<>();
		
        for(Task t : taskGraph.getNodeSet())
        {
            inputFiles.addAll(t.getInputFiles());
			outputFiles.addAll(t.getOutputFiles());
			
            inputFiles.removeAll(t.getOutputFiles());
			outputFiles.removeAll(t.getInputFiles());
        }
	}
    
 
    
	
	
	/////////////////////////Getters and Setters//////////////////////
	
    public List<WorkflowFile> getInputFiles()
    {
        return new ArrayList(inputFiles);
    }
	
    public String getWorkingDirSuffix()
    {
        return "wf_"+uuid+"/";
    }
    
    
    /**
     * Return task queue ordered by the task dependency that the parent
     * task will come before the child task
     * @return 
     */
    public LinkedList<Task> getTaskQueue()
    {
        return taskGraph.getOrderedNodes();
    }
    
    public double getCumulatedExecTime()
    {
        double time = 0;
        for(Task t : taskGraph.getNodeSet())
        {
            time += t.getEstimatedExecTime();
        }
        return time;
    }
	
	public Workflow getSubworkflow(String name, Collection<Task> tasksInSubWf)
	{
		Workflow w = new Workflow(name);
		for(Task t : tasksInSubWf)
		{
			for(Task c : this.getChild(t))
			{
				w.taskGraph.addNodes(t, c);
			}
			for(Task p : this.getParent(t))
			{
				w.taskGraph.addNodes(p, t);
			}
		}
        w.finalizeWorkflow();
		return w;
	}
    
    public static void main(String[] args) throws DBException, FileNotFoundException
    {
        Utils.disableDB();
        Workflow wf = WorkflowFactory.fromDAX("/home/orachun/Desktop/dag.xml");
        
    }

	public long getStartTime()
	{
		return startTime;
	}

	public void setStartTime(long startTime)
	{
		this.startTime = startTime;
	}

	public long getScheduledTime()
	{
		return scheduledTime;
	}

	public void setScheduledTime(long scheduledTime)
	{
		this.scheduledTime = scheduledTime;
	}

	public long getFinishedTime()
	{
		return finishedTime;
	}

	public void setFinishedTime(long finishedTime)
	{
		this.finishedTime = finishedTime;
	}

	public long getEstimatedFinishedTime()
	{
		return estimatedFinishedTime;
	}

	public void setEstimatedFinishedTime(long estimatedFinishedTime)
	{
		this.estimatedFinishedTime = estimatedFinishedTime;
	}
	
	public char getStatus()
	{
		return status;
	}

	public void setStatus(char status)
	{
		this.status = status;
	}
    
    public static boolean isFinished()
    {
        return isFinished();
    }
    public String getUUID()
    {
        return uuid;
    }
	
    @Override
    public String toString()
    {
        return name;
    }
    @Override
    public boolean equals(Object o)
    {
        return o instanceof Workflow && this.uuid.equals(((Workflow)o).uuid);
    }

    @Override
    public int hashCode()
    {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.name);
        return hash;
    }
    public int getTotalTasks()
    {
        return taskGraph.size();
    }

    public String getName()
    {
        return name;
    }
	
	
    public Iterable<Task> getTaskIterator()
    {
        return new Iterable<Task>(){
            @Override
            public Iterator<Task> iterator()
            {
                return taskGraph.getNodeSet().iterator();
            }
        };
    }
	
	public Set<Task> getTaskSet()
	{
		return taskGraph.getNodeSet();
	}

	public Set<Task> getParent(Task n)
	{
		return taskGraph.getParent(n);
	}

	public Set<Task> getChild(Task n)
	{
		return taskGraph.getChild(n);
	}
	
	public Set<Task> getStartTasks()
	{
		return taskGraph.getRoots();
	}
	
	public Set<Task> getEndTasks()
	{
		return taskGraph.getLeaves();
	}
	
	
}
