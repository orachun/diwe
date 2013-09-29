/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package removed;

import workflowengine.workflow.Task;
import java.io.File;
import workflowengine.communication.FileTransferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import workflowengine.communication.Communicator;
import workflowengine.communication.HostAddress;
import workflowengine.communication.message.Message;
import workflowengine.utils.db.DBRecord;
import workflowengine.schedule.Schedule;
import workflowengine.schedule.scheduler.Scheduler;
import workflowengine.schedule.SchedulingSettings;
import workflowengine.schedule.fc.CostOptimizationFC;
import workflowengine.schedule.fc.FC;
import workflowengine.utils.db.DBException;
import workflowengine.utils.Logger;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 * Task manager of the execution site to manage running of tasks in each worker.
 *
 * @author Orachun
 */
public class TaskManager extends Service
{

    public static final Logger logger = Utils.getLogger();
    public static int TASK_DELAY_THRESHOLD = 60;
    public static int RESCHEDULING_INTERVAL = 60;
//    private static Scheduler sch = null;
    private static TaskManager tm = null;
    private HostAddress nearestEsp;
    private Properties p = new Properties();
    private int dispatchCheckInterval = 15000;
    private final Object DISPATCH_LOCK_OBJ = new Object();
    private final Object RESCHEDULING_LOCK_OBJ = new Object();
    private boolean rescheduling = false;
    private long lastReschedulingTime = Utils.time();
    private int waitingDispatchThread = 0;

    private TaskManager() throws IOException, ClassNotFoundException, 
            IllegalAccessException, InstantiationException
    {
        prepareComm();
        getScheduler();
        addr = new HostAddress(Utils.getPROP(), "task_manager_host", "task_manager_port");
        nearestEsp = new HostAddress(Utils.getPROP(), "nearest_esp_host", "nearest_esp_port");
        comm.setTemplateMsgParam(Message.PARAM_FROM_SOURCE, Message.SOURCE_TASK_MANAGER);
        comm.setListeningPort(addr.getPort());
        comm.startServer();
        startDispatchThread();
    }
    
    private Scheduler getScheduler() 
    {
        try
        {
            Class c = ClassLoader.getSystemClassLoader().loadClass(Utils.getProp("scheduler").trim());
            Scheduler s = (Scheduler) c.newInstance();
            return s;
        }
        catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex)
        {
            java.util.logging.Logger.getLogger(TaskManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    @Override
    protected void prepareComm()
    {
        comm = new Communicator("Task Manager")
        {
            @Override
            public void handleMessage(Message msg)
            {
//            logger.log("Received msg from "+msg.getParam(Message.PARAM_FROM));
//            logger.log(msg.toString());
                switch (msg.getType())
                {
                    case Message.TYPE_SUBMIT_WORKFLOW:
                        submitWorkflow(msg);
                        break;
                    case Message.TYPE_DISPATCH_TASK_REQUEST:
                        dispatchTaskRequest(msg);
                        break;
                    case Message.TYPE_SHUTDOWN:
                        System.exit(0);
                        break;
                    case Message.TYPE_NONE: break;
                    default:
                        System.out.println("The message type " + msg.getType() + " from " + msg.get(Message.PARAM_FROM) + " is not "
                                + "handled.");
                        System.out.println("-------------");
                        System.out.println(msg.toString());
                        System.out.println("-------------");
                }
            }
        };
    }

    private void dispatchTaskRequest(Message msg)
    {
        int wfid = msg.getInt("wfid");
        if (Workflow.isFinished(wfid))
        {
            printSummary(wfid, msg.getLong("bytes_transferred"));
        }
        else 
        {
            if(Utils.getIntProp("dynamic") == 1 && !rescheduling)
            {
                synchronized(RESCHEDULING_LOCK_OBJ)
                {
                    if (isRescheduleNeeded(msg))
                    {
                        rescheduling = true;
                        System.out.println("Rescheduling...");
                        logger.log("Rescheduling ...");
                        reschedule(wfid);
                        logger.log("Done.");
                        System.out.println("Done.");
                        rescheduling = false;
                    }
                }
            }
            dispatchTask();
        }
    }
    private synchronized void printSummary(int wfid, long bytes_transferred)
    {
        logger.log("Workflow execution is completed.");
        logger.log("Scheduler:\t"+Utils.getProp("scheduler"));
        logger.log("Deadline%:\t"+Utils.getProp(CostOptimizationFC.PROP_DEADLINE));
        DBRecord rec = DBRecord.select("SELECT * from workflow where wfid='"+wfid+"'").get(0);
        logger.log("Workflow:\t"+rec.get("name"));
        logger.log("Scheduling time:\t"+(rec.getInt("scheduled_at")-rec.getInt("started_at")));
        int makespan = rec.getInt("finished_at")-rec.getInt("scheduled_at");
        logger.log("Makespan:\t"+makespan);
        
        double totalUsage = 0;
        List<DBRecord> list = DBRecord.select("SELECT total_usage FROM worker");
        for(DBRecord r : list)
        {
            double usage = r.getDouble("total_usage");
//            double remain = usage % 60;
//            if(remain > 0)
//            {
//                usage += (60 - remain);
//            }
            totalUsage += usage;
        }
        double commCost = (bytes_transferred*Utils.BYTE)* (Utils.getDoubleProp(ExecSite.PROP_TRANSFER_UCOST)/Utils.GB);
        logger.log("Comm cost:\t"+commCost);
        double execCost = (totalUsage*Utils.getDoubleProp(Worker.PROP_UNIT_COST));
        logger.log("Exec cost:\t"+execCost);
        double totalCost = commCost + execCost;
        logger.log("Total cost:\t"+totalCost);
        
        double deadline = Utils.getDoubleProp(CostOptimizationFC.PROP_DEADLINE)*Workflow.getCumulatedExecTime(wfid);
        double CONSTANT_PENALTY = Utils.getDoubleProp(CostOptimizationFC.PROP_CONSTANT_PENALTY);
        double WEIGHT = Utils.getDoubleProp(CostOptimizationFC.PROP_WEIGHTED_PENALTY);
        
        logger.log("Deadline:\t"+deadline);
        
        if (makespan > deadline)
        {
            double PenaltyFunction = CONSTANT_PENALTY + WEIGHT * (makespan - deadline);
            totalCost+= PenaltyFunction;
        }
        
        logger.log("Net cost:\t"+totalCost);
        System.out.println("Workflow Done");
    }
    public static TaskManager startService()
    {
        try
        {
            if (tm == null)
            {
                tm = new TaskManager();
            }
            logger.log("Task manager is started.");
            return tm;
        }
        catch (IOException | ClassNotFoundException | IllegalAccessException | InstantiationException ex)
        {
            logger.log("Cannot start task manager: ", ex);
            return null;
        }
    }

    private void startDispatchThread()
    {
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (true)
                {
                    int count = DBRecord.select("select count(*) as count from _task_to_dispatch").get(0).getInt("count");
                    if(count > 0)
                    {
                        dispatchTask();
                    }
                    try
                    {
                        Thread.sleep(dispatchCheckInterval);
                    }
                    catch (InterruptedException ex)
                    {
                        logger.log("Cannot sleep for dispatching task.", ex);
                    }
                }
            }
        }).start();
    }

    /**
     * Check whether the current workflow should be rescheduled.
     * @param msg message contains info of the task that is just finished
     * @return true if the start time or finish time is later than estimated
     * for TASK_DELAY_THRESHOLD seconds
     */
    public boolean isRescheduleNeeded(Message msg)
    {
//        if(Utils.time() - lastReschedulingTime > RESCHEDULING_INTERVAL)
//        {
//            lastReschedulingTime = Utils.time();
//            return true;
//        }
//        return false;
        
        
        
//        count++;
//        return count == 2;
        
        //TODO: implement this
        if(rescheduling || Utils.time() - lastReschedulingTime < RESCHEDULING_INTERVAL)
        {
            return false;
        }
        
        int start = msg.getInt("start");
        int end = msg.getInt("end");
        int tid = msg.getInt("tid");
        DBRecord s = DBRecord.select("schedule", new DBRecord("schedule").set("tid", tid)).get(0);
        
        int est_start = s.getInt("estimated_start");
        int est_finish = s.getInt("estimated_finish");
        return (Math.abs(start - est_start) > TASK_DELAY_THRESHOLD) || (Math.abs(end - est_finish) > TASK_DELAY_THRESHOLD);
//        return false;
    }
    
    public void reschedule(int wfid)
    {
        //TODO: implement this
        Workflow wf = Workflow.open(Utils.getProp("working_dir") + wfid + "/" + wfid + ".wfobj");

        //Build fixed mapping of completed tasks
        List<DBRecord> res = DBRecord.select(
                "SELECT s.tid, w.uuid "
                + "FROM workflow_task t JOIN schedule s ON t.tid = s.tid "
                + "JOIN worker w on w.wkid = s.wkid "
                + "WHERE status = 'C' AND t.wfid='" + wfid + "'");
        HashMap<Task, Worker> fixedMapping = new HashMap<>(res.size());
        for (DBRecord r : res)
        {
            Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
            t.setProp("fixed", true);
            Worker w = Worker.getWorkerFromDB(r.get("uuid"));
            fixedMapping.put(t, w);
        }

        //Calculate new schedule
        FC fc = new CostOptimizationFC(
                    Utils.getDoubleProp(CostOptimizationFC.PROP_DEADLINE)*Workflow.getCumulatedExecTime(wfid) - (Utils.time()-Workflow.getScheduledTime(wfid)), 
                    Utils.getDoubleProp(CostOptimizationFC.PROP_CONSTANT_PENALTY), 
                    Utils.getDoubleProp(CostOptimizationFC.PROP_WEIGHTED_PENALTY)
                    );
        
        System.out.println("getting new schedule...");
        SchedulingSettings ss = new SchedulingSettings(wf, getExecSite(), fixedMapping, fc);
        Schedule currentSch = (Schedule)Utils.readFromFile("schedule_wf"+wfid+".sch");
        ss.setParam("current_schedule", currentSch);
        Schedule newSchedule = getScheduler().getSchedule(ss);
        System.out.println("evaluating new schedule...");
        newSchedule.evaluate();
        
        int currentFinishTime = Workflow.getEstimatedFinishTime(wfid);
        int newFinishTime = (int)Math.round(newSchedule.getMakespan() + estimateMigrationTime());
        
        System.out.println("evaluating new schedule ...");
//        if (currentFinishTime > newFinishTime)
        
        double curFit = curSch.getFitness();
        double newFit = newSchedule.getFitness();
        
//        if((curFit-newFit)/curFit > 0)
        
        {
            System.out.println("executing new schedule...");
            curSch = newSchedule;
            Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
            synchronized (DISPATCH_LOCK_OBJ)
            {
                //Update new schedule
                updateScheduleForWorkflow(wf, newSchedule, Utils.time());

                //Checkpoint and suspend all running tasks
                List<Message> msgs = broadcastToWorkers(new Message(Message.TYPE_SUSPEND_TASK), true);
                
                for (Message m : msgs)
                {
                    comm.getResponseMessage(m);
                    dispatchTask();
                }
                lastReschedulingTime = Utils.time();
            }
        }
    }
    
    public double estimateMigrationTime()
    {
        List<DBRecord> list = DBRecord.select("SELECT current_tid, estopr, start "
                + "FROM worker w JOIN workflow_task t ON w.current_tid=t.tid "
                );
        double total = 0;
        ExecSite es = getExecSite();
        for(DBRecord r : list)
        {
            if(r.getInt("current_tid") == -1)
            {
                continue;
            }
            Task t = Task.getWorkflowTaskFromDB(r.getInt("current_tid"));
            
            long currentTime = Utils.time();
            int startTime = r.getInt("start");
            long elapsedTime = currentTime - startTime;
            int estopr = r.getInt("estopr");
            double progress = elapsedTime/(double)estopr;;
            progress = Math.min(1, progress);
            total += progress * es.getTransferTime(t.getOutputFileUUIDs());
        }
        return total;
    }

    public void submitWorkflow(Message msg)
    {
        String daxFile = msg.get("dax_file");
        logger.log("Workflow " + daxFile + " is submitted.");
        Properties p = (Properties)msg.getObject("properties");
        Utils.setProp(p);
        p.list(System.out);
        
        logger.log("Reading workflow1...");
        TASK_DELAY_THRESHOLD = Utils.getIntProp("task_delay_threshold");
        RESCHEDULING_INTERVAL = Utils.getIntProp("rescheduling_interval");
        logger.log("TASK_DELAY_THRESHOLD="+TASK_DELAY_THRESHOLD);
        logger.log("RESCHEDULING_INTERVAL="+RESCHEDULING_INTERVAL);
        
        Workflow w;
        logger.log("Reading workflow3...");
        String inputFileDir = msg.get("input_dir");
        logger.log("Reading workflow4...");
        if(daxFile.endsWith("dummy"))
        {
            w = Workflow.fromDummyDAX(daxFile, false);
            logger.log("Creating dummy input files...");
            w.createDummyInputFiles(inputFileDir);
            logger.log("Done.");
        }
        else
        {
            w = Workflow.fromDAX(daxFile);
        }

        logger.log("Uploading input files...");
        String remoteInputFileDir = Utils.getProp("exec_site_file_storage_dir") + w.getWorkingDirSuffix();
        try
        {
            comm.sendFilesInDir(nearestEsp.getHost(), nearestEsp.getPort(), remoteInputFileDir, inputFileDir);
        }
        catch (FileTransferException ex)
        {
            logger.log("Cannot upload input files for " + w.getName() + ": " + ex.getMessage());
            return;
        }
        
        List<WorkflowFile> files = w.getInputFiles();
        List<WorkflowFile> registeringFiles = new LinkedList<>();
        for (File f : new File(inputFileDir).listFiles())
        {
            WorkflowFile wff = WorkflowFile.getFile(f.getName(), f.length()*Utils.BYTE, WorkflowFile.TYPE_FILE);
//            registerFile(new WorkflowFile[]
//            {
//                wff
//            }, nearestEsp, true);
            registeringFiles.add(wff);
            
        }
        registerFile(registeringFiles.toArray(new WorkflowFile[registeringFiles.size()]), nearestEsp, true);
        files.removeAll(registeringFiles);
        
        logger.log("Done.");
        if(files.isEmpty())
        {
            execWorkflow(w);
        }
        else
        {
            logger.log("Cannot execute the submitted workflow: some input file is missing.");
            for(WorkflowFile f : files)
            {
                logger.log(f.getName());
            }
        }
        System.gc();
    }

    private static Schedule curSch;
    public void execWorkflow(Workflow wf) throws DBException
    {
        logger.log("Scheduling the workflow " + wf.getName() + ".");
        Schedule schedule;

        Utils.setPropIfNotExist(CostOptimizationFC.PROP_DEADLINE, "-1");
        Utils.setPropIfNotExist(CostOptimizationFC.PROP_CONSTANT_PENALTY, "0");
        Utils.setPropIfNotExist(CostOptimizationFC.PROP_WEIGHTED_PENALTY, "0");
        
        //Synchronized to shedule one workflow at a time
        long scheduledTime;
        synchronized (this)
        {
            FC fc = new CostOptimizationFC(
                    Utils.getDoubleProp(CostOptimizationFC.PROP_DEADLINE)*wf.getCumulatedExecTime(), 
                    Utils.getDoubleProp(CostOptimizationFC.PROP_CONSTANT_PENALTY), 
                    Utils.getDoubleProp(CostOptimizationFC.PROP_WEIGHTED_PENALTY)
                    );
            Workflow.setStartedTime(wf.getDbid(), Utils.time());
            schedule = getScheduler().getSchedule(new SchedulingSettings(wf, getExecSite(), fc));
            scheduledTime = Utils.time();
            Workflow.setScheduledTime(wf.getDbid(), scheduledTime);
            logger.log("Makespan:\t"+schedule.getMakespan());
            logger.log("Cost:\t"+schedule.getCost());
            logger.log("Fitness:\t"+schedule.getFitness());
            int estFinish = (int)Math.round(Utils.time()+schedule.getMakespan());
            Workflow.setEstimatedFinishTime(wf.getDbid(), estFinish);
        }
        schedule.evaluate();
        setScheduleForWorkflow(wf, schedule, scheduledTime);
        
        System.gc();
        curSch = schedule;
        
        Utils.writeToFile(schedule, "schedule_wf"+wf.getDbid()+".sch");
        logger.log("Workflow " + wf.getName() + " is scheduled.");
        dispatchTask();
    }

    public void updateScheduleForWorkflow(Workflow wf, Schedule schedule, long startTime) throws DBException
    {
        SchedulingSettings ss = schedule.getSettings();
        for (Task t : wf.getTaskIterator())
        {
            if(!ss.isFixedTask(t))
            {
                Worker w = schedule.getWorkerForTask(t);
                DBRecord.update("UPDATE schedule "
                        + "SET wkid='" + w.getDbid() + "', "
                        + "estimated_start='" + Math.round(startTime + schedule.getEstimatedStart(t)) + "', "
                        + "estimated_finish='" + Math.round(startTime + schedule.getEstimatedFinish(t)) + "' "
                        + "WHERE tid='" + t.getDbid() + "'");
            }
        }
    }

    public void setScheduleForWorkflow(Workflow wf, Schedule schedule, long startTime) throws DBException
    {
        for (Task t : wf.getTaskIterator())
        {
            Worker w = schedule.getWorkerForTask(t);
            new DBRecord("schedule",
                    "wkid", w.getDbid(),
                    "tid", t.getDbid(),
                    "estimated_start", Math.round(startTime+schedule.getEstimatedStart(t)),
                    "estimated_finish", Math.round(startTime+schedule.getEstimatedFinish(t))).insert();
        }
    }

    /**
     * Dispatch ready tasks
     */
    public void dispatchTask()
    {
        if (waitingDispatchThread > 1)
        {
            return;
        }
        waitingDispatchThread++;
        synchronized (DISPATCH_LOCK_OBJ)
        {
            waitingDispatchThread--;
            List<DBRecord> results = DBRecord.select("_task_to_dispatch", new DBRecord());
            List<Thread> dispatchThreads = new LinkedList<>();
            for (DBRecord res : results)
            {
                final DBRecord r = res;
                final Task t = Task.getWorkflowTaskFromDB(r.getInt("tid"));
                final Message msg = new Message(Message.TYPE_DISPATCH_TASK);
                String dir = Utils.getProp("exec_site_file_storage_dir") + t.getWorkingDirSuffix();
                msg.set("cmd", t.getCmd());
                msg.set("task_name", t.getName());
                msg.set("task_namespace", t.getNamespace());
                msg.set("tid", t.getDbid());
                msg.set("wfid", t.getWfdbid());
                msg.set(Message.PARAM_WORKER_UUID, r.get("uuid"));
                msg.set("input_files", t.getInputFileUUIDs());
                msg.set("file_dir", dir);
                msg.set("output_files", t.getOutputFileUUIDs());
                if (t.getStatus() == Task.STATUS_SUSPENDED)
                {
                    msg.set("migrate", true);
                }
                else
                {
                    msg.set("migrate", false);
                }

                /**
                 * Start new thread for each task dispatching to transfer input
                 * files parallelly.
                 */
                Thread dispatchThread = new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            logger.log("Uploading input files and checkpoint files (if needed) "
                                    + "for task " + t.getName() + " to " + r.get("uuid") + "...");

                            //Upload input files
                            boolean isUploadComplete = uploadInputFilesForTaskToEsp(t, r.get("esp_hostname"));

                            //Upload checkpoint files if this task is migrated
//                            if (t.getStatus() == Task.STATUS_SUSPENDED)
//                            {
//                                isUploadComplete = isUploadComplete && uploadCheckpointFiles(t.getDbid(), r.get("esp_hostname"));
//                            }
                            
                            //Start dispatch task
                            if (isUploadComplete)
                            {
                                logger.log("Done.");
                                logger.log("Dispatching task " + t.getName() + " to " + r.get("esp_hostname") + ":" + r.get("port") + "...");
                                Message response = comm.sendForResponseSync(r.get("esp_hostname"), r.getInt("esp_port"), addr.getPort(), msg.copy());
                                if (response.getBoolean("complete"))
                                {
                                    logger.log("Done.");
                                    Task.updateTaskStatus(t.getDbid(), -1, -1, -1, Task.STATUS_DISPATCHED);
                                }
                                else
                                {
                                    logger.log("Fail dispatching task " + t.getName() + ". The worker is busy.");
                                }
                            }
                            else
                            {
                                logger.log("Fail uploading file.");
                            }
                        }
                        catch (IOException | FileTransferException ex)
                        {
                            logger.log("Cannot send execution request of " + t.getName() + " to " + r.get("hostname") + ": " + ex.getMessage());
                        }
                    }
                });
                dispatchThread.start();
                dispatchThreads.add(dispatchThread);
            }
            try
            {
                for(Thread th : dispatchThreads)
                {
                    th.join();
                }
            }
            catch (InterruptedException ex)
            {
                logger.log("", ex);
            }
        }
        System.gc();
    }

    /**
     * Upload required input files for the given task from any proxy to the
     * given espHost name. This method will block until all transfers are
     * stopped.
     *
     * @param t task
     * @param espHost execution site proxy hostname
     * @throws FileTransferException
     */
    private boolean uploadInputFilesForTaskToEsp(Task t, String espHost) throws FileTransferException
    {
        //Select input files needed by the task t
        List<DBRecord> files = DBRecord.select(
                "SELECT f.fid, f.name "
                + "FROM workflow_task_file tf join `file` f on tf.fid = f.fid "
                + "WHERE `type`='I' AND tid='" + t.getDbid() + "'");
        String dir = Utils.getProp("exec_site_file_storage_dir") + t.getWorkingDirSuffix();
        LinkedList<Message> sentMsgs = new LinkedList<>();
        for (DBRecord f : files)
        {
            String fname = f.get("name");
            //Whether the file is in the espHost
            boolean fileExist = DBRecord.select("SELECT fid "
                    + "FROM exec_site_file esf "
                    + "JOIN exec_site es ON esf.esid = es.esid "
                    + "WHERE es.hostname='" + espHost + "' "
                    + "AND fid = '" + f.get("fid") + "'").size() > 0;
            if (!fileExist)
            {
                try
                {
                    //Select the execution site that contain the file f
                    DBRecord r = DBRecord.select("SELECT hostname, port "
                            + "FROM exec_site_file esf JOIN exec_site es ON esf.esid = es.esid "
                            + "WHERE esf.fid = '" + f.get("fid") + "'").get(0);

                    //Send message to that execution site to transfer required files
                    //to espHpst
                    Message msg = new Message(Message.TYPE_FILE_UPLOAD_REQUEST);
                    msg.set("filename", fname);
                    msg.set("dir", dir);
                    msg.set("upload_to", espHost);
                    msg.set("fid", f.get("fid"));
                    msg.set("file", WorkflowFile.getFileFromDB(f.getInt("fid")));
                    logger.log("Request file " + fname + " from " + r.get("hostname") + " to " + espHost);
                    comm.sendForResponseAsync(r.get("hostname"), r.getInt("port"), addr.getPort(), msg);
                    logger.log("Done");
                    sentMsgs.add(msg);
                }
                catch (IndexOutOfBoundsException ex)
                {
                    throw new FileTransferException("No execution site containing the file " + fname + ".");
                }
                catch (IOException ex)
                {
                    throw new FileTransferException("Cannot request input file " + fname + ": " + ex.getMessage());
                }
            }
        }

        boolean complete = true;
        for (Message msg : sentMsgs)
        {
            Message res = comm.getResponseMessage(msg);
            if (!res.getBoolean("upload_complete"))
            {
                complete = complete && false;
                logger.log("", ((Exception) res.getObject("exception")));
            }
        }
        return complete;
    }

    public boolean uploadCheckpointFiles(int tid, String espHost)
    {
        try
        {
            DBRecord rec = DBRecord.select(
                    "SELECT es.hostname, es.port, esc.path "
                    + "FROM exec_site_checkpoint esc "
                    + "JOIN exec_site es ON esc.esid = es.esid "
                    + "WHERE esc.tid = '" + tid + "' "
                    + "ORDER BY esc.checkpointed_at DESC LIMIT 1")
                    .get(0);
            Message msg = new Message(Message.TYPE_CHECKPOINT_FILE_UPLOAD_REQUEST)
                    .set("path", rec.get("path"))
                    .set("upload_to", espHost);
            Message response = comm.sendForResponseSync(rec.get("hostname"), rec.getInt("port"), addr.getPort(), msg);
            return response.getBoolean("complete");
        }
        catch (IndexOutOfBoundsException ex)
        {
            logger.log("No host with checkpoint for tid " + tid + " found.");
            return false;
        }
        catch (IOException ex)
        {
            logger.log("Cannot send message.", ex);
            return false;
        }
    }

    public ExecSite getExecSite() throws DBException
    {
        List<DBRecord> workers = DBRecord.select("worker", "SELECT uuid FROM worker");
        ExecSite es = new ExecSite();
        for (DBRecord w : workers)
        {
            es.addWorker(Worker.getWorkerFromDB(w.get("uuid")));
        }
        return es;
    }

    public ArrayList<Message> broadcastToWorkers(Message msg, boolean waitForResponse)
    {
        List<DBRecord> workers = DBRecord.select("worker", "select uuid, esp_hostname, esp_port from worker");
        ArrayList<Message> msgs = new ArrayList<>();
        for (DBRecord w : workers)
        {
            msg.set(Message.PARAM_WORKER_UUID, w.get("uuid"));
            Message m = msg.copy();
            msgs.add(m);
            try
            {
                if (waitForResponse)
                {
                    comm.sendForResponseAsync(
                            w.get("esp_hostname"), 
                            w.getInt("esp_port"), 
                            this.addr.getPort(), 
                            m
                            );
                }
                else
                {
                    comm.sendMessage(w.get("esp_hostname"), w.getInt("esp_port"), m);
                }
            }
            catch (IOException ex)
            {
                logger.log("Cannot broadcast message to " + w.get("uuid") + ": " + ex.getMessage());
            }
        }
        if (waitForResponse)
        {
            return msgs;
        }
        else
        {
            return null;
        }
    }
    
    
}
