/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.UniqueMap;
import workflowengine.utils.db.DBException;
import workflowengine.utils.Utils;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.MongoDB;
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
	protected boolean isSubworkflow = false;
	
	private Set<Task> allTasks = null;	//All tasks for transfer over servers
	private Set<WorkflowFile> allFiles = null; //All files for transfer over servers

	public Workflow(String name, String uuid)
	{
		this.name = name;
		this.uuid = uuid;
		Cacher.cache(uuid, this);
	}
	
	public void cache()
	{
		Cacher.cache(uuid, this);
	}

	public void createDummyInputFiles()
	{
		String wfid = getSuperWfid();
		for (String fuuid : this.inputFiles)
		{
			WorkflowFile f = WorkflowFile.get(fuuid);
			if (!f.getName().equals("dummy"))
			{
				String outfile = Utils.getProp("working_dir") + "/" + f.getName(wfid);
				File file = new File(outfile);
				file.getParentFile().mkdirs();
				Utils.bash("truncate --size " + (int) (Math.round(f.getSize())) + " " + outfile);
			}
		}
	}

	/**
	 * Prepare workflow to be used further. Gather workflow input/output files
	 */
	public void generateInputOutputFileList()
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
	public Queue<String> getTaskQueueByOrder()
	{
		return taskGraph.getOrderedNodes();
	}
	
	public Queue<String> getTaskQueueByPriority()
	{
		LinkedList<String> tasks = new LinkedList<>(getTaskSet());
		Collections.sort(tasks, new Comparator<String>() {

			@Override
			public int compare(String o1, String o2)
			{
				double t1 = Task.get(o1).getPriority();
				double t2 = Task.get(o2).getPriority();
				return t1 > t2 ? -1 : t1 == t2 ? 0 : 1;
			}
		});
		return tasks;
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
		w.isSubworkflow = true;
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
		w.generateInputOutputFileList();
		w.superWfid = this.getSuperWfid();
		return w;
	}

//	public long getStartTime()
//	{
//		return startTime;
//	}
//
//	public void setStartTime()
//	{
//		this.startTime = Utils.time();
//	}

	public long getScheduledTime()
	{
		return scheduledTime;
	}

	public void setScheduledTime()
	{
		this.scheduledTime = Utils.time();
		setStatus(STATUS_SCHEDULED);
	}

	public long getFinishedTime()
	{
		return finishedTime;
	}

	public void setCompletedTime()
	{
		this.finishedTime = Utils.time();
		setStatus(STATUS_COMPLETED);
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

	public void setSubmittedTime()
	{
		this.submitted = Utils.time();
		setStatus(STATUS_SUBMITTED);
	}

	public long getSubmittedTime()
	{
		return submitted;
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
		if(supposeReadyTasks == null || supposeReadyTasks.isEmpty())
		{
			return isTaskReady(tid);
		}
		for (String parent : this.getParent(tid))
		{
			if (!supposeReadyTasks.contains(parent)
					&& Task.get(parent).getStatus().status != TaskStatus.STATUS_COMPLETED)
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

	public boolean isSubworkflow()
	{
		return isSubworkflow;
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

	public boolean isFileActive(String fid)
	{
		for (String tid : getTaskSet())
		{
			Task t = Task.get(tid);
			char s = t.getStatus().status;
			if (s != TaskStatus.STATUS_EXECUTING
					&& s != TaskStatus.STATUS_COMPLETED
					&& t.getInputFiles().contains(fid))
			{
				return true;
			}
		}
		return false;
	}

	public static Workflow get(String uuid)
	{
		return (Workflow) Cacher.get(Workflow.class, uuid);
	}

	public static Workflow getInstance(Object key)
	{
		DBObject obj = MongoDB.WORKFLOW.findOne(new BasicDBObject("wfid", key.toString()));
		if (obj == null)
		{
			return null;
		}
		Workflow wf = new Workflow((String) obj.get("name"), (String) obj.get("wfid"));
		wf.cumulatedEstimatedExecTime = (long) obj.get("cumulated_time");
		wf.estimatedFinishedTime = (long) obj.get("est_finish");
		wf.finishedTime = (long) obj.get("finished_at");
		wf.scheduledTime = (long) obj.get("scheduled_at");
		wf.startTime = (long) obj.get("started_at");
		wf.status = ((String) obj.get("status")).charAt(0);
		wf.submitted = (long) obj.get("submitted");
		wf.superWfid = (String) obj.get("superwfid");
		wf.isSubworkflow = (boolean) obj.get("is_subworkflow");

		BasicDBList tasks = (BasicDBList) obj.get("tasks");
		for (Object o : tasks)
		{
			DBObject t = (DBObject) o;
			BasicDBList parents = (BasicDBList) t.get("parents");
			for (Object po : parents)
			{
				wf.taskGraph.addNodes((String) po, (String) t.get("tid"));
			}
			BasicDBList children = (BasicDBList) t.get("children");
			for (Object co : children)
			{
				wf.taskGraph.addNodes((String) t.get("tid"), (String) co);
			}
		}
		wf.generateInputOutputFileList();
		return wf;

	}

	@Override
	public void save()
	{
		BasicDBObject bson = new BasicDBObject()
				.append("wfid", uuid)
				.append("superwfid", superWfid)
				.append("name", name)
				.append("submitted", submitted)
				.append("status", String.valueOf(status))
				.append("started_at", startTime)
				.append("finished_at", finishedTime)
				.append("est_finish", estimatedFinishedTime)
				.append("cumulated_time", cumulatedEstimatedExecTime)
				.append("is_subworkflow", isSubworkflow);

		BasicDBList taskList = new BasicDBList();

		for (String tid : getTaskSet())
		{
			BasicDBList parents = new BasicDBList();
			parents.addAll(getParent(tid));
			BasicDBList children = new BasicDBList();
			children.addAll(getChild(tid));

			BasicDBObject task = new BasicDBObject("tid", tid)
					.append("parents", parents)
					.append("children", children);
			taskList.add(task);
		}
		bson.append("tasks", taskList);
		MongoDB.WORKFLOW.update(new BasicDBObject("wfid", uuid), bson, true, false);

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
		System.gc();
	}

	public void prepareRemoteSubmit()
	{
		allTasks = new HashSet<>();
		allFiles = new HashSet<>();

		for (String t : getTaskSet())
		{
			Task task = Task.get(t);
			allTasks.add(task);
			for (String f : task.getInputFiles())
			{
				allFiles.add(WorkflowFile.get(f));
			}
			for (String f : task.getOutputFiles())
			{
				allFiles.add(WorkflowFile.get(f));
			}
		}
	}

	/**
	 * Must be called after submitted immediately
	 */
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
		Cacher.cache(this.uuid, this);
	}

	public boolean containsTask(String tid)
	{
		return taskGraph.getNodeSet().contains(tid);
	}

	
	public Workflow getSubWorkflowOfRemainTasks()
	{
		Set<String> remainTasks = new HashSet<>();
		for(String tid : getTaskSet())
		{
			if(Task.get(tid).getStatus().status != TaskStatus.STATUS_COMPLETED)
//			if(Task.get(tid).getStatus().status == TaskStatus.STATUS_WAITING)
			{
				remainTasks.add(tid);
			}
		}
		return getSubworkflow(this.name, remainTasks);
	}
	
	public void saveStat(double totalCost)
	{
		MongoDB.WORKFLOW_SUBMIT_INFO.insert(new BasicDBObject()
				.append("workflow_name", getName())
				.append("submited", Utils.formatDateTime(getSubmittedTime()))
				.append("scheduled", Utils.formatDateTime(getScheduledTime()))
				.append("completed", Utils.formatDateTime(getFinishedTime()))
				.append("makespan", getFinishedTime()-getScheduledTime())
				.append("cost", totalCost)
				.append("dynamic", Utils.getProp("dynamic"))
				.append("deadline", Utils.getProp("fc_deadline"))
				.append("scheduler", Utils.getProp("scheduler"))
				
				);
	}
	
	public void saveToDaxFile(String filepath)
	{
		try
		{
			UniqueMap<Integer, String> tids = UniqueMap.fromArray(getTaskSet().toArray(new String[0]));
			PrintWriter pw = new PrintWriter(filepath);
			pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			pw.println("<adag>");
			for(String tid : getTaskSet())
			{
				Task t = Task.get(tid);
				pw.printf("<job id=\"ID%05d\" name=\"%s\" runtime=\"%f\"> \n", tids.getKey(tid), t.getName(), t.getEstimatedExecTime());
				for(String fid : t.getInputFiles())
				{
					WorkflowFile f = WorkflowFile.get(fid);
					pw.printf("\t<uses file=\"%s\" link=\"input\" size=\"%d\"/>\n", f.getName(), f.getSize());
				}
				for(String fid : t.getOutputFiles())
				{
					WorkflowFile f = WorkflowFile.get(fid);
					pw.printf("\t<uses file=\"%s\" link=\"input\" size=\"%d\"/>\n", f.getName(), f.getSize());
				}
				pw.printf("</job>\n");
			}
			
			for(String tid : getTaskSet())
			{
				Set<String> parents = getParent(tid);
				if(!parents.isEmpty())
				{
					pw.printf("<child ref=\"ID%05d\">\n", tids.getKey(tid));
					for(String ptid : parents)
					{
						pw.printf("\t<parent ref=\"ID%05d\" />\n", tids.getKey(ptid));
					}
					pw.printf("</child>\n");
				}
			}
			
			pw.println("</adag>");
			pw.close();
		}
		catch (FileNotFoundException ex)
		{
			Logger.getLogger(Workflow.class.getName()).log(Level.SEVERE, null, ex);
		}
		
	}
	
	
	public static void randomBalancedWorkflow(String daxFilePaht, String name, int tasks, int levels, int averageRuntime)
	{
		Random r = new Random();
		Workflow wf = new Workflow(name, Utils.uuid());
		int tasksPerLevel = (int)Math.round(tasks/(double)levels);
		String rootTid = Utils.uuid();
		int runtime = (int)Math.round((r.nextGaussian()*0.2*averageRuntime)+averageRuntime);
		Task t = new Task(wf.uuid, "ROOT", "dummy;"+runtime, runtime, rootTid, 
				TaskStatus.waitingStatus(rootTid));
		wf.taskGraph.addNode(rootTid);
		for(int i=0;i<tasksPerLevel;i++)
		{
			String parent = rootTid;
			for(int j=0;j<levels;j++)
			{
				String tid = Utils.uuid();
				runtime = (int)Math.round((r.nextGaussian()*averageRuntime)+averageRuntime);
				t = new Task(wf.uuid, "T"+i+"-"+j, "dummy;"+runtime, runtime, tid,
						TaskStatus.waitingStatus(tid));
				wf.taskGraph.addNodes(parent, tid);
				parent = tid;
			}
		}
		wf.saveToDaxFile(daxFilePaht);
	}
	
	public static void randomUnbalancedWorkflow(String daxFilePaht, String name, int tasks, int averageRuntime)
	{
		List<String> tids = new ArrayList<>(tasks);
		Random r = new Random();
		Workflow wf = new Workflow(name, Utils.uuid());
		String rootTid = Utils.uuid();
		int runtime = (int)Math.round((r.nextGaussian()*averageRuntime)+averageRuntime);
		Task t = new Task(wf.uuid, "ROOT", "dummy;"+runtime, runtime, rootTid, 
				TaskStatus.waitingStatus(rootTid));
		tids.add(rootTid);
		wf.taskGraph.addNode(rootTid);
		for(int i=0;i<tasks-1;i++)
		{
			String parent = tids.get(r.nextInt(tids.size()));
			String tid = Utils.uuid();
			runtime = (int)Math.round((r.nextGaussian()*0.2*averageRuntime)+averageRuntime);
			t = new Task(wf.uuid, "T"+i, "dummy;"+runtime, runtime, tid,
					TaskStatus.waitingStatus(tid));
			wf.taskGraph.addNodes(parent, tid);
			tids.add(tid);			
		}
		wf.saveToDaxFile(daxFilePaht);
	}
	
	
	public static void main(String[] args) throws DBException, FileNotFoundException
	{
		randomBalancedWorkflow(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4]));
	}
}
