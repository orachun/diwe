package workflowengine.workflow;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.MongoDB;
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
	private double priority = 1;
	private String superWfid;
	private String ckptFid = null;

	public Task(String superWfid, String name, String cmd, double estimateExecTime, String uuid, TaskStatus status)
	{
		this.superWfid = superWfid;
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

	public Set<String> getInputFiles()
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
		Set<String> in = Task.get(childTaskUUID).getInputFiles();
		Set<String> files = new HashSet<>();
		for (String f : in)
		{
			if (out.contains(f))
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

	@Override
	public String toString()
	{
		return "Task[" + uuid + "]";
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

	public String getSuperWfid()
	{
		return superWfid;
	}

	public void setCkptFid(String ckptFid)
	{
		this.ckptFid = ckptFid;
	}

	public String getCkptFid()
	{
		return ckptFid;
	}

	
	
	public static Task get(String taskUUID)
	{
		return (Task) Cacher.get(Task.class, taskUUID);
	}

	public static Task getInstance(Object key)
	{
		DBObject obj = MongoDB.TASK.findOne(new BasicDBObject("tid", key.toString()));
		if (obj == null)
		{
			return null;
		}
		TaskStatus s = new TaskStatus(
				(String) obj.get("tid"),
				((String) obj.get("status")).charAt(0),
				(int) obj.get("exit_value"),
				"",
				(long) obj.get("start"),
				(long) obj.get("finish"));
		Task t = new Task(
				(String) obj.get("super_wfid"),
				(String) obj.get("name"),
				(String) obj.get("cmd"),
				(double) obj.get("estopr"),
				(String) obj.get("tid"),
				s);
		t.ckptFid = (String)obj.get("ckpt_fid");
		t.setPriority((double) obj.get("priority"));
		BasicDBList inputs = (BasicDBList) obj.get("input");
		for (Object o : inputs)
		{
			t.inputs.add((String) o);
		}
		BasicDBList outputs = (BasicDBList) obj.get("output");
		for (Object o : outputs)
		{
			t.inputs.add((String) o);
		}
		return t;

	}

	@Override
	public void save()
	{

		BasicDBList inputList = new BasicDBList();
		BasicDBList outputList = new BasicDBList();
		inputs.addAll(getInputFiles());
		outputs.addAll(getOutputFiles());
		BasicDBObject obj = new BasicDBObject()
				.append("tid", uuid)
				.append("name", name)
				.append("cmd", cmd)
				.append("status", String.valueOf(status.status))
				.append("estopr", (long) estimateExecTime)
				.append("start", status.start)
				.append("finish", status.finish)
				.append("exit_value", status.retVal)
				.append("priority", priority)
				.append("input", inputList)
				.append("output", outputList)
				.append("super_wfid", superWfid)
				.append("ckpt_fid", ckptFid);
		MongoDB.TASK.update(new BasicDBObject("tid", uuid), obj, true, false);

	}
}
