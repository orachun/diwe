package workflowengine.workflow;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import workflowengine.utils.db.DBRecord;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.Savable;

/**
 *
 * @author Orachun
 */

public class Task implements Serializable, Comparable<Task>, Savable
{
    private String uuid;
    private double estimateExecTime;
    private String cmd;
	private String name;
	private TaskStatus status;
    private Set<String> inputs = new HashSet<>(); //WorkflowFile
    private Set<String> outputs = new HashSet<>(); //WorkflowFile
	private double priority = -1;


    public Task(String name, String cmd, double estimateExecTime, String uuid, TaskStatus status)
    {
		this.name = name;
        this.estimateExecTime = estimateExecTime;
        this.cmd = cmd;
		this.uuid = uuid;
		this.status = status;
		Cacher.cache(uuid, this);
    }
	
    
    public void addInputFile(WorkflowFile f)
    {
        inputs.add(f.getUUID());
    }

    public void addOutputFile(WorkflowFile f)
    {
        outputs.add(f.getUUID());
    }
	
    public Set<String> getInputFileUUIDs()
    {
        return new HashSet<>(inputs);
    }
    public Set<String> getOutputFiles()
    {
        return new HashSet<>(outputs);
    }

    public Set<String> getOutputFileUUIDsForTask(String childTaskUUID)
    {
        Set<String> out = this.getOutputFiles();
        Set<String> in = Task.get(childTaskUUID).getInputFileUUIDs();
        Set<String> files = new HashSet<>();
        for (String f : in)
        {
            if(out.contains(f))
			{
				files.add(f);
			}
        }
        return files;
    }

	public void setStatus(TaskStatus s)
	{
		this.status = s;
	}
	
	
	
	public boolean isReady()
	{
		return DBRecord.select("_not_ready_task", new DBRecord().set("tid", this.uuid)).size() == 0;
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
//		hash = 29 * hash + Objects.hashCode(this.wfUUID);
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

//    public String getWfUUID()
//    {
//        return wfUUID;
//    }

    public void setCmd(String cmd)
    {
        this.cmd = cmd;
    }

    public String getCmd()
    {
        return cmd;
    }

	public String getName()
	{
		return name;
	}
	
	public TaskStatus getStatus()
	{
		return status;
	}
	
	
	public double getPriority()
	{
		return priority;
	}

	public void setPriority(double p)
	{
		this.priority = p;
	}

	
	
	public static Task get(String taskUUID)
	{
		return (Task)Cacher.get(Task.class, taskUUID);
	}
	
	public static Task getInstance(Object key)
	{
		try{
			DBRecord record = DBRecord.select("task", 
					new DBRecord()
					.set("tid", key.toString())).get(0);
			TaskStatus s = new TaskStatus(
					record.get("tid"),
					record.get("status").charAt(0),
					record.getInt("exit_value"),
					"",
					record.getLong("start"),
					record.getLong("finish"));
			Task t = new Task(
					record.get("name"),
					record.get("cmd"),
					record.getDouble("estopr"),
					record.get("tid"),
					s);
			t.setPriority(record.getDouble("priority"));
			List<DBRecord> files = DBRecord.select("workflow_task_file",
					new DBRecord().set("tid", t.uuid));
			for (DBRecord r : files)
			{
				if (r.get("type").charAt(0) == 'I')
				{
					t.inputs.add(r.get("fid"));
				}
				else
				{
					t.outputs.add(r.get("fid"));
				}
			}
			return t;
		}
		catch (IndexOutOfBoundsException e){return null;}
	}

	
	private static final String[] taskKeys = new String[]{"tid"};
	private static final String[] taskFileKeys = new String[]{"tid", "fid"};
	@Override
	public void save()
	{
		new DBRecord("task")
				.set("tid", uuid)
				.set("name", name)
				.set("cmd", cmd)
				.set("status", String.valueOf(status.status))
				.set("estopr", (long)estimateExecTime)
				.set("start", status.start)
				.set("finish", status.finish)
				.set("exit_value", status.retVal)
				.set("priority", priority)
				.upsert(taskKeys);
		
		new DBRecord("workflow_task_file")
				.set("tid", uuid)
				.delete();
		
		for(String f : inputs)
		{
			WorkflowFile wff = (WorkflowFile)Cacher.get(f);
			if(wff!=null)
			{
				wff.save();
			}
			new DBRecord("workflow_task_file")
					.set("tid", uuid)
					.set("fid", f)
					.set("type", "I")
					.upsert(taskFileKeys);
		}
		for(String f : outputs)
		{
			WorkflowFile wff = (WorkflowFile)Cacher.get(f);
			if(wff!=null)
			{
				wff.save();
			}
			new DBRecord("workflow_task_file")
					.set("tid", uuid)
					.set("fid", f)
					.set("type", "O")
					.upsert(taskFileKeys);
		}
	}
	
	
}
