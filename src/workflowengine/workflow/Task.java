package workflowengine.workflow;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import workflowengine.utils.DBException;

/**
 *
 * @author Orachun
 */

public class Task implements Serializable, Comparable<Task>
{
    private String uuid;
    private String wfUUID;
    private double estimateExecTime;
    private String cmd;
    private LinkedList<WorkflowFile> inputs = null;
    private LinkedList<WorkflowFile> outputs = null;

    private Task(double estimateExecTime, String wfUUID, String cmd)
    {
        this.estimateExecTime = estimateExecTime;
        this.wfUUID = wfUUID;
        this.cmd = cmd;
    }
    
    public void addInputFile(WorkflowFile f) throws DBException
    {
        inputs.add(f);
    }

    public void addOutputFile(WorkflowFile f) throws DBException
    {
        outputs.add(f);
    }
	
    public Set<WorkflowFile> getInputFiles() throws DBException
    {
        return new HashSet<>(inputs);
    }
    public Set<WorkflowFile> getOutputFiles() throws DBException
    {
        return new HashSet<>(outputs);
    }

    public Set<WorkflowFile> getOutputFilesForTask(Task t)
    {
        Set<WorkflowFile> out = this.getOutputFiles();
        Set<WorkflowFile> in = t.getInputFiles();
        Set<WorkflowFile> files = new HashSet<>();
        for (WorkflowFile f : in)
        {
            if(out.contains(f))
			{
				files.add(f);
			}
        }
        return files;
    }

	
	
	
	public boolean isReady()
	{
		throw new UnsupportedOperationException("Not implemented yet.");
	}
	
	
	
    @Override
    public String toString()
    {
        return "Task["+uuid+"]";
    }

    @Override
    public boolean equals(Object o)
    {
        return (o instanceof Task && this.toString().equals(o.toString()));
    }

	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 29 * hash + Objects.hashCode(this.uuid);
		hash = 29 * hash + Objects.hashCode(this.wfUUID);
		hash = 29 * hash + Objects.hashCode(this.cmd);
		return hash;
	}

    @Override
    public int compareTo(Task o)
    {
        int h1 = hashCode();
        int h2 = o.hashCode();
        if (h1 < h2)
        {
            return -1;
        }
        if (h1 == h2)
        {
            return 0;
        }
        return 1;
    }
	
	public String getUUID()
	{
		return uuid;
	}
	
	public double getEstimatedExecTime()
	{
		return estimateExecTime;
	}

    public String getWfUUID()
    {
        return wfUUID;
    }

    public void setCmd(String cmd)
    {
        this.cmd = cmd;
    }

    public String getCmd()
    {
        return cmd;
    }

}
