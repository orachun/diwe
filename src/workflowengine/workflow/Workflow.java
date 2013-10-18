/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.File;
import java.io.FileNotFoundException;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import workflowengine.utils.db.DBException;
import workflowengine.utils.Utils;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.DBRecord;
import workflowengine.utils.db.Savable;
import workflowengine.utils.simplegraph.DirectedGraph;

/**
 *
 * @author Orachun
 */
public class Workflow implements Serializable, Savable
{

	public static final char STATUS_SUBMITTED = 'W';
	public static final char STATUS_SCHEDULED = 'S';
	public static final char STATUS_COMPLETED = 'C';
	public boolean isDummy = false;
	protected String uuid;
	protected char status = STATUS_SUBMITTED;
	protected DirectedGraph<String> taskGraph = new DirectedGraph<>();
	protected String name = "";
	protected Set<String> inputFiles = new HashSet<>(); //WorkflowFile
	protected Set<String> outputFiles = new HashSet<>(); //WorkflowFile
	protected String superWfid = "";
	protected long submitted = -1;
	protected long startTime = -1;
	protected long scheduledTime = -1;
	protected long finishedTime = -1;
	protected long estimatedFinishedTime = -1;
	protected boolean isFinished = false;
	protected long cumulatedEstimatedExecTime = -1;
	protected Set<Task> allTasks = null;	//All tasks for transfer over servers
	protected Set<WorkflowFile> allFiles = null; //All files for transfer over servers

	public Workflow(String name, String uuid)
	{
		this.name = name;
		this.uuid = uuid;
		Cacher.cache(uuid, this);
	}

	public void createDummyInputFiles()
	{
		String superWfid = getSuperWfid();
		for (String fuuid : this.inputFiles)
		{
			WorkflowFile f = WorkflowFile.get(fuuid);
			if (!f.getName().equals("dummy"))
			{
				String outfile = Utils.getProp("working_dir")+"/"+f.getName(superWfid);
				File file = new File(outfile);
				file.getParentFile().mkdirs();
				Utils.bash("truncate --size " + (int)(Math.round(f.getSize())) + " " + outfile, false);
			}
		}
	}

	/**
	 * Prepare workflow to be used further.
	 */
	public void finalizeWorkflow()
	{
		HashSet<String> tmpInputFiles = new HashSet<>();
		HashSet<String> tmpOutputFiles = new HashSet<>();

		for (String taskUUID : taskGraph.getNodeSet())
		{
			Task t = (Task) Cacher.get(Task.class, taskUUID);
			tmpInputFiles.addAll(t.getInputFiles());
			tmpOutputFiles.addAll(t.getOutputFiles());
		}

		inputFiles = new HashSet<>(tmpInputFiles);
		outputFiles = new HashSet<>(tmpOutputFiles);

		inputFiles.removeAll(tmpOutputFiles);
		outputFiles.removeAll(tmpInputFiles);
	}

	/////////////////////////Getters and Setters//////////////////////
	public Set<String> getInputFiles()
	{
		return new HashSet(inputFiles);
	}

	public Set<String> getOutputFiles()
	{
		return new HashSet(outputFiles);
	}

	/**
	 * Return task queue ordered by the task dependency that the parent task
	 * will come before the child task
	 *
	 * @return
	 */
	public Queue<String> getTaskQueue()
	{
		return taskGraph.getOrderedNodes();
	}

	public double getCumulatedExecTime()
	{
		double time = 0;
		for (String taskUUID : taskGraph.getNodeSet())
		{
			time += Task.get(taskUUID).getEstimatedExecTime();
		}
		return time;
	}

	public Workflow getSubworkflow(String name, Collection<String> tasksInSubWf)
	{
		Workflow w = new Workflow(name, Utils.uuid());
		for (String taskUUID : tasksInSubWf)
		{
			w.taskGraph.addNode(taskUUID);
			for (String cTaskUUID : this.getChild(taskUUID))
			{
				if (tasksInSubWf.contains(cTaskUUID))
				{
					w.taskGraph.addNodes(taskUUID, cTaskUUID);
				}
			}
			for (String pTaskUUID : this.getParent(taskUUID))
			{
				if (tasksInSubWf.contains(pTaskUUID))
				{
					w.taskGraph.addNodes(pTaskUUID, taskUUID);
				}
			}
		}
		w.finalizeWorkflow();
		w.superWfid = this.getSuperWfid();
		return w;
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

	public void setSubmitted(long submitted)
	{
		this.submitted = submitted;
	}

	public static boolean isFinished()
	{
		return isFinished();
	}

	public String getUUID()
	{
		return uuid;
	}
	public static boolean isTaskReady(String tid, String wfid)
	{
		return Workflow.get(wfid).isTaskReady(tid);
	}
	public boolean isTaskReady(String tid)
	{
		for (String parent : this.getParent(tid))
		{
			if (Task.get(parent).getStatus().status != TaskStatus.STATUS_COMPLETED)
			{
				return false;
			}
		}
		return true;
	}
	
	public boolean isTaskReady(String tid, Set<String> supposeReadyTasks)
	{
		for (String parent : this.getParent(tid))
		{
			if (!supposeReadyTasks.contains(parent) && 
					Task.get(parent).getStatus().status != TaskStatus.STATUS_COMPLETED)
			{
				return false;
			}
		}
		return true;
	}
	

	@Override
	public String toString()
	{
		return name;
	}

	@Override
	public boolean equals(Object o)
	{
		return o instanceof Workflow && this.uuid.equals(((Workflow) o).uuid);
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

	public Set<String> getTaskSet()
	{
		return taskGraph.getNodeSet();
	}

	public Set<String> getParent(String n)
	{
		return taskGraph.getParent(n);
	}

	public Set<String> getChild(String n)
	{
		return taskGraph.getChild(n);
	}

	public Set<String> getStartTasks()
	{
		return taskGraph.getRoots();
	}

	public Set<String> getEndTasks()
	{
		return taskGraph.getLeaves();
	}

	/**
	 * Return the name of the working directory (not full path) for this
	 * workflow
	 *
	 * @return
	 */
	public String getSuperWfid()
	{
		return (superWfid.isEmpty() ? uuid : superWfid);
	}

	public boolean checkIfFinished()
	{
		if (isFinished)
		{
			return true;
		}
		for (String t : getEndTasks())
		{
			if (Task.get(t).getStatus().status != TaskStatus.STATUS_COMPLETED)
			{
				return false;
			}
		}
		isFinished = true;
		return true;
	}

	public static Workflow get(String uuid)
	{
		return (Workflow) Cacher.get(Workflow.class, uuid);
	}

	public static Workflow getInstance(Object key)
	{
		try
		{
			DBRecord r = DBRecord.select("workflow",
					new DBRecord().set("wfid", key.toString())).get(0);
			Workflow wf = new Workflow(r.get("name"), r.get("wfid"));
			wf.cumulatedEstimatedExecTime = r.getLong("cumulated_time");
			wf.estimatedFinishedTime = r.getLong("est_finish");
			wf.finishedTime = r.getLong("finished_at");
			wf.scheduledTime = r.getLong("scheduled_at");
			wf.startTime = r.getLong("started_at");
			wf.status = r.get("status").charAt(0);
			wf.submitted = r.getLong("submitted");

			List<DBRecord> res = DBRecord.select("workflow_task_depen",
					new DBRecord().set("wfid", key.toString()));
			for (DBRecord r2 : res)
			{
				wf.taskGraph.addNodes(r2.get("parent"), r2.get("child"));
			}
			wf.superWfid = r.get("superwfid");
			return wf;
		}
		catch (IndexOutOfBoundsException e)
		{
			return null;
		}
	}
	private static final String[] workflowKeys = new String[]
	{
		"wfid"
	};
	private static final String[] workflowTaskKeys = new String[]
	{
		"wfid", "tid"
	};
	private static final String[] taskDepenKeys = new String[]
	{
		"parent", "child"
	};

	@Override
	public void save()
	{
		new DBRecord("workflow")
				.set("wfid", uuid)
				.set("superwfid", superWfid)
				.set("name", name)
				.set("submitted", submitted)
				.set("status", String.valueOf(status))
				.set("started_at", startTime)
				.set("finished_at", finishedTime)
				.set("est_finish", estimatedFinishedTime)
				.set("cumulated_time", cumulatedEstimatedExecTime)
				.upsert(workflowKeys);

		if (allTasks != null)
		{
			for (Task task : allTasks)
			{
				task.save();
				Cacher.cache(task.getUUID(), task);
			}
		}
		if (allFiles != null)
		{
			for (WorkflowFile f : allFiles)
			{
				f.save();
				Cacher.cache(f.getUUID(), f);
			}
		}



		Queue<String> taskQueue = this.getTaskQueue();
		while (!taskQueue.isEmpty())
		{
			String task = taskQueue.poll();
			new DBRecord("workflow_task")
					.set("wfid", this.uuid)
					.set("tid", task)
					.upsert(workflowTaskKeys);

			for (String child : this.getChild(task))
			{
				new DBRecord("workflow_task_depen")
						.set("parent", task)
						.set("child", child)
						.set("wfid", uuid)
						.upsert(taskDepenKeys);
			}
		}
		System.gc();
	}

	public void prepareRemoteSubmit()
	{
		allTasks = new HashSet<>();
		allFiles = new HashSet<>();

		for (String t : getStartTasks())
		{
			Task task = Task.get(t);
			for (String f : task.getInputFiles())
			{
				allFiles.add(WorkflowFile.get(f));
			}
		}

		for (String t : taskGraph.getNodeSet())
		{
			Task task = Task.get(t);
			allTasks.add(task);
			for (String f : task.getOutputFiles())
			{
				allFiles.add(WorkflowFile.get(f));
			}
		}
	}

	public void finalizedRemoteSubmit()
	{
		if (allFiles != null)
		{
			for (WorkflowFile f : allFiles)
			{
				Cacher.cache(f.getUUID(), f);
			}
			allFiles = null;
		}
		if (allTasks != null)
		{
			for (Task task : allTasks)
			{
				Cacher.cache(task.getUUID(), task);
			}
			allTasks = null;
		}
	}

	public boolean containsTask(String tid)
	{
		return taskGraph.getNodeSet().contains(tid);
	}

	public static void main(String[] args) throws DBException, FileNotFoundException
	{
		Workflow wf = WorkflowFactory.fromDummyDAX("/drive-d/Dropbox/Work (1)/Workflow Thesis/ExampleDAGs/Inspiral_30.xml", "Inspiral_30");
		Cacher.flushAll();
	}
}
