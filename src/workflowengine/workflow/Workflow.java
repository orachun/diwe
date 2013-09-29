/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.File;
import java.io.FileNotFoundException;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import removed.TaskManager;
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
    
    
    protected String uuid;
    protected char status = STATUS_SUBMITTED;
    protected DirectedGraph<String> taskGraph = new DirectedGraph<>();
    protected String name = "";
    protected Set<String> inputFiles = new HashSet<>(); //WorkflowFile
    protected Set<String> outputFiles = new HashSet<>(); //WorkflowFile

	protected long submitted = -1;
	protected long startTime = -1;
	protected long scheduledTime = -1;
	protected long finishedTime = -1;
	protected long estimatedFinishedTime = -1;
	protected boolean isFinished = false;
	protected long cumulatedEstimatedExecTime = -1;
	
    public Workflow(String name, String uuid) throws DBException
    {
        this.name = name;
		this.uuid = uuid;
		Cacher.cache(uuid, this);
    }
    
    public void createDummyInputFiles(String destDir)
    {
        for(String fuuid : this.inputFiles)
        {
			WorkflowFile f = (WorkflowFile)Cacher.get(WorkflowFile.class, fuuid);
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
		
        for(String taskUUID : taskGraph.getNodeSet())
        {
			Task t = (Task)Cacher.get(Task.class, taskUUID);
            inputFiles.addAll(t.getInputFileUUIDs());
			outputFiles.addAll(t.getOutputFileUUIDs());
			
            inputFiles.removeAll(t.getOutputFileUUIDs());
			outputFiles.removeAll(t.getInputFileUUIDs());
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
    public Queue<String> getTaskQueue()
    {
        return taskGraph.getOrderedNodes();
    }
    
    public double getCumulatedExecTime()
    {
        double time = 0;
        for(String taskUUID : taskGraph.getNodeSet())
        {
            time += Task.get(taskUUID).getEstimatedExecTime();
        }
        return time;
    }
	
	public Workflow getSubworkflow(String name, Collection<String> tasksInSubWf)
	{
		Workflow w = new Workflow(name, Utils.uuid());
		for(String taskUUID : tasksInSubWf)
		{
			for(String cTaskUUID : this.getChild(taskUUID))
			{
				w.taskGraph.addNodes(taskUUID, cTaskUUID);
			}
			for(String pTaskUUID : this.getParent(taskUUID))
			{
				w.taskGraph.addNodes(pTaskUUID, taskUUID);
			}
		}
        w.finalizeWorkflow();
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
	
//	public static Workflow getInstant(Object key)
//	{
//		try{
//			DBRecord r = DBRecord.select("workflow",
//					new DBRecord().set("wfid", key.toString())).get(0);
//			Workflow wf = new Workflow(r.get("name"), r.get("wfid"));
//			wf.cumulatedEstimatedExecTime = r.getLong("cumulated_time");
//			wf.estimatedFinishedTime = r.getLong("est_finish");
//			wf.finishedTime = r.getLong("finished_at");
//			wf.scheduledTime = r.getLong("scheduled_at");
//			wf.startTime = r.getLong("started_at");
//			wf.status = r.get("status").charAt(0);
//			wf.submitted = r.getLong("submitted");
//			return wf;
//		}
//		catch (IndexOutOfBoundsException e)
//		{
//			return null;
//		}
//	}

	
	private static final String[] workflowKeys = new String[]{"wfid"};
	private static final String[] taskDepenKeys = new String[]{"parent", "child"};
	@Override
	public void save()
	{
		new DBRecord("workflow")
				.set("wfid", uuid)
				.set("name", name)
				.set("submitted", submitted)
				.set("status", String.valueOf(status))
				.set("started_at", startTime)
				.set("finished_at", finishedTime)
				.set("est_finish", estimatedFinishedTime)
				.set("cumulated_time", cumulatedEstimatedExecTime)
				.upsert(workflowKeys);
		
		//Save all tasks
		for(String taskUUID: getTaskSet())
		{
			Savable task = Cacher.get(taskUUID);
			if(task != null)
			{
				task.save();
			}
		}
		
		Queue<String> taskQueue = this.getTaskQueue();
		while(!taskQueue.isEmpty())
		{
			String task = taskQueue.poll();
			for(String child : this.getChild(task))
			{
				new DBRecord("workflow_task_depen")
						.set("parent", task)
						.set("child", child)
						.set("wfid", uuid)
						.upsert(taskDepenKeys);
			}
		}
		
	}
	
	
    public static void main(String[] args) throws DBException, FileNotFoundException
    {
        Workflow wf = WorkflowFactory.fromDummyDAX("/drive-d/Dropbox/Work (1)/Workflow Thesis/ExampleDAGs/Inspiral_30.xml");
		Cacher.flushAll();
    }
}
