/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package removed;

import workflowengine.workflow.Task;
import workflowengine.communication.FileTransferException;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import javax.naming.OperationNotSupportedException;
import workflowengine.communication.Communicator;
import workflowengine.communication.HostAddress;
import workflowengine.communication.message.Message;
import workflowengine.utils.Logger;
import workflowengine.utils.Utils;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author Orachun
 */
public class TaskExecutor extends Service
{

    public static final short STATUS_IDLE = 1;
    public static final short STATUS_BUSY = 2;
    public static final String CKPT_OUTPUT_FILE_SUFFIX = ".ckpt_output";
    private static final int COOR_PORT = Utils.getIntProp("coordinator_port");
    private static final int HEARTBEAT_INTERVAL = 10000; //10 seconds
    private static Logger logger = Utils.getLogger();
    private static TaskExecutor te = null;
    private Properties p = new Properties();
    private short status = STATUS_IDLE;
    private int totalUsage = 0;
    private String currentTaskName;
    private String currentProcName;
    private String currentWorkingDir;
    private int currentTaskDbid = -1;
    private long currentTaskStart;
    private long currentTaskEnd;
    private Message currentRequestMsg;
    private Process currentProcess;
    private boolean isSuspensed = false;
    private String uuid;
    private HostAddress espAddr;
    private int port;
    private double cpu = 1;
    private final Object SUSPEND_LOCK = new Object();

    @Override
    protected void prepareComm()
    {
        comm = new Communicator("Worker")
        {
            @Override
            public void handleMessage(Message msg)
            {
//            logger.log("Received msg from "+msg.getParam(Message.PARAM_FROM));
//            logger.log(msg.toString());
                switch (msg.getType())
                {
                    case Message.TYPE_DISPATCH_TASK:
                        Thread.currentThread().setName("TYPE_DISPATCH_TASK");
                        long start = Utils.time();
                        exec(msg);
                        totalUsage += Utils.time() - start;
                        break;
                    case Message.TYPE_GET_NODE_STATUS:
                        Thread.currentThread().setName("TYPE_GET_NODE_STATUS");
                        updateNodeStatus();
                        break;
                    case Message.TYPE_SUSPEND_TASK:
                        Thread.currentThread().setName("TYPE_SUSPEND_TASK");
                        suspendCurrentTask(msg);
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

    private TaskExecutor() throws IOException
    {
        prepareComm();
        espAddr = new HostAddress(Utils.getPROP(), "exec_site_proxy_host", "exec_site_proxy_port");
        port = (Utils.getIntProp("task_executor_port"));
        uuid = Utils.uuid();
        comm.setListeningPort(port);
        comm.setTemplateMsgParam(Message.PARAM_WORKER_UUID, uuid);
        comm.setTemplateMsgParam(Message.PARAM_FROM_SOURCE, Message.SOURCE_TASK_EXECUTOR);
        comm.startServer();
        startHeartBeat();
    }

    public static TaskExecutor startService()
    {
        try
        {
            if (te == null)
            {
                te = new TaskExecutor();
            }
            logger.log("Task executor is started.");
            return te;
        }
        catch (IOException ex)
        {
            logger.log("Cannot start task executor: " + ex.getLocalizedMessage());
            return null;
        }
    }

    private void startHeartBeat()
    {
        Thread heartBeat = new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (true)
                {
                    try
                    {
                        updateNodeStatus();
                        Thread.sleep(HEARTBEAT_INTERVAL);
                    }
                    catch (InterruptedException ex)
                    {
                    }
                }
            }
        }, "HEARTBEAT_THREAD");
        heartBeat.start();
    }

    public void updateNodeStatus()
    {
        updateNodeStatus(false);
    }

    public void updateNodeStatus(boolean sync)
    {
        Message msg = new Message(Message.TYPE_UPDATE_NODE_STATUS);
        msg.set("status", status);

//        File defaultStorage = new File(Utils.getProp("working_dir"));
//        msg.setParam("free_space", defaultStorage.getFreeSpace()); //in bytes
        msg.set("free_space", 1); //in bytes

        msg.set("current_tid", currentTaskDbid);
        msg.set("free_memory", getFreeMemory());
        msg.set("cpu", getCPU());
        msg.set("total_usage", totalUsage);
        msg.set(Message.PARAM_WORKER_PORT, this.port).set("comment","TaskExecutor:161");
        try
        {
            if (sync)
            {
//                System.out.println("Sending node status: "+msg.getUUID());
                comm.sendForResponseSync(espAddr, this.port, msg);
            }
            else
            {
                comm.sendMessage(espAddr, msg);
            }
        }
        catch (IOException | InterruptedException ex)
        {
            logger.log("Cannot update node status.", ex);
        }
    }

    public double getFreeMemory()
    {
        return 1;
//        try
//        {
//            Process p = Runtime.getRuntime().exec(new String[]
//            {
//                "/bin/bash", "-c", "grep \"MemFree:\" /proc/meminfo | awk '{print $2}'"
//            });
//            return Integer.parseInt(new BufferedReader(new InputStreamReader(p.getInputStream())).readLine());
//        }
//        catch (IOException ex)
//        {
//            return -1;
//        }
    }

    public double getCPU()
    {
        return 1;
//        if(cpu == -1)
//        {
//            try
//            {
//                Process p = Runtime.getRuntime().exec(new String[]
//                {
//                    "/bin/bash", "-c", "echo 0 $(lscpu | grep \"CPU MHz:\" | awk '{print $3}' |  sed 's#^#+#' ) | bc"
//                });
//                cpu = Double.parseDouble(new BufferedReader(new InputStreamReader(p.getInputStream())).readLine());
//            }
//            catch (IOException ex)
//            {
//                cpu = -1;
//            }
//        }
//        return cpu;
    }

    private void setIdle()
    {
        currentTaskName = "";
        currentTaskDbid = -1;
        currentProcName = "";
        currentWorkingDir = "";
        status = STATUS_IDLE;
        currentTaskStart = -1;
        currentTaskEnd = -1;
        currentRequestMsg = null;
    }

    private String getCkptDir()
    {
//        return currentWorkingDir+currentTaskDbid+"-ckpt-files/";
        String dir = "/home/we/ckpt-files/"+currentTaskDbid+"/";
        Utils.createDir(dir);
        return dir;
    }
    
    private String[] prepareCmd(Message msg)
    {
        String cmdPrefix;
        currentProcName = msg.get("cmd").split(";")[0];
        if (msg.getBoolean("migrate"))
        {
            cmdPrefix = "dmtcp_restart;-q;-p;" + COOR_PORT + ";";
        }
        else
        {
            cmdPrefix = "dmtcp_checkpoint;-q;-p;" + COOR_PORT 
                    + ";--ckptdir;"
                    + getCkptDir() +";";
        }
        StringBuilder cmd = new StringBuilder();

//        cmd.append(cmdPrefix).append(currentWorkingDir).append(msg.get("task_namespace")).append("/").append(msg.get("cmd"));
        
        cmd.append(cmdPrefix).append(currentWorkingDir).append(msg.get("cmd").trim());

//        if (msg.get("migrate") != null)
//        {
//            cmd.append(";-_condor_restart;").append(currentTaskName).append(".ckpt");
//        }
        return cmd.toString().split(";");
    }

    /**
     * Download input files using information of execution message
     *
     * @param msg
     */
    private void downloadInputFiles(Message msg) throws FileTransferException
    {
        WorkflowFile[] files = (WorkflowFile[]) msg.getObject("input_files");
        String fromRemoteDir = msg.get("file_dir");
//        String espHost = Utils.getProp("exec_site_proxy_host");
//        SFTPClient client = SFTPUtils.getSFTP(espHost);
        for (WorkflowFile f : files)
        {
            logger.log("Downloading input files " + f.getName() + " for " + currentTaskName + "...");
            try
            {
//                if (f.getType() == WorkflowFile.TYPE_DIRECTIORY)
//                {
//                    String localFileDir = Utils.getParentPath(currentWorkingDir + f.getName());
////                    Utils.createDir(localFilePath);
//                    comm.getDir(espAddr.getHost(), espAddr.getPort(), fromRemoteDir + f.getName(), localFileDir);
////                    client.getFolder(remoteDir + f.getName(), localFilePath, null);
//                }
//                else
                if (!Utils.isFileExist(currentWorkingDir + f.getName()))
                {
                    comm.getFile(espAddr.getHost(), espAddr.getPort(), fromRemoteDir + f.getName(), currentWorkingDir + f.getName());
//                    client.getFile(f.getName(), remoteDir, currentWorkingDir);
                }
                if (f.getType() == WorkflowFile.TYPE_CHECKPOINT_FILE)
                {
                    File file = new File(currentWorkingDir + f.getName());
                    File newFile = new File(file.getParent() + "/" + f.getName().replace(CKPT_OUTPUT_FILE_SUFFIX, ""));
                    file.renameTo(newFile);
                }
            }
            catch (FileTransferException ex)
            {
                logger.log("Cannot download file " + f.getName() + ".", ex);
                throw ex;
            }
            logger.log("Done.");
        }
    }

    private void uploadOutputFiles(Message msg) throws FileTransferException
    {
        WorkflowFile[] files = (WorkflowFile[]) msg.getObject("output_files");
        String remoteDir = msg.get("file_dir");
//        String espHost = Utils.getProp("exec_site_proxy_host");
//        SFTPClient client = SFTPUtils.getSFTP(espHost);
        for (WorkflowFile f : files)
        {
            logger.log("Uploading output files " + f.getName() + " from task " + currentTaskName + "...");
            String localFilePath = currentWorkingDir + f.getName();

            try
            {
//                if (Utils.isDir(localFilePath))
//                {
//                    comm.sendDir(espAddr.getHost(), espAddr.getPort(), remoteDir, localFilePath);
//                }
//                else
                {
                    comm.sendFile(espAddr.getHost(), espAddr.getPort(), remoteDir + f.getName(), localFilePath);
                }
            }
            catch (FileTransferException ex)
            {
                logger.log("Cannot upload output file " + f.getName() + ".", ex);
                throw ex;
            }
            logger.log("Done.");
        }
        registerFile(files, espAddr, false);
//        Message outputFileMsg = new Message(Message.TYPE_REGISTER_FILE);
//        outputFileMsg.set("files", files);
//        outputFileMsg.set("is_workflow_file", true);
//        try
//        {
//            comm.sendMessage(espAddr, outputFileMsg);
//        }
//        catch (IOException ex)
//        {
//            logger.log("Cannot send file registering message to " + espAddr + ": " + ex.getMessage(), ex);
//        }
    }

    private void downloadExecutable(Message msg) throws FileTransferException
    {
        String namespace = msg.get("task_namespace");
//        String dir = currentWorkingDir + namespace + "/";
        String dir = currentWorkingDir + namespace + "/";
        if (!Utils.isFileExist(dir))
        {
//            long time = System.currentTimeMillis();
            Utils.createDir(dir);
            comm.getDir(Utils.getProp("code_repository_host"), espAddr.getPort(), Utils.getProp("code_repository_dir") + namespace + "/", currentWorkingDir);

//            SFTPUtils.getSFTP(Utils.getProp("code_repository_host"))
//                    .getFolder(Utils.getProp("code_repository_dir") + namespace + "/", currentWorkingDir, null);
            Utils.setExecutableInDirSince(dir, -1);
        }
        try
        {
            Utils.createProcessBuilder(new String[]
            {
                "bash",
                "-c",
                "mv " + dir + "* " + currentWorkingDir
            }).start().waitFor();
        }
        catch (IOException | InterruptedException ex)
        {
            throw new FileTransferException(ex);
        }
//        String execName = msg.getParam("cmd").split(";")[0];
//        SFTPUtils.getSFTP(Utils.getProp("code_repository_host"))
//                .getFile(execName, Utils.getProp("code_repository_dir"), currentWorkingDir);
//        new File(currentWorkingDir+execName).setExecutable(true);
    }

    private Process startProcess(String[] cmds, Message msg) throws IOException
    {
//        ProcessBuilder pb = new ProcessBuilder(cmds).directory(new File(
//                currentWorkingDir));
//        pb.redirectError(new File(currentWorkingDir + currentTaskName + ".stderr"));
//        pb.redirectOutput(new File(currentWorkingDir + currentTaskName + ".stdout"));
//        String path = pb.environment().get("PATH") + ":" + currentWorkingDir+":"+currentWorkingDir+msg.getParam("task_namespace");
//        pb.environment().put("PATH", path);

        ProcessBuilder pb = Utils.createProcessBuilder(cmds,
                currentWorkingDir,
                currentWorkingDir + currentTaskName + ".stdout",
                currentWorkingDir + currentTaskName + ".stderr",
                currentWorkingDir + msg.get("task_namespace"));

        currentTaskStart = Utils.time();

        StringBuilder cmdString = new StringBuilder();
        List<String> cs = pb.command();
        for (int i = 0; i < cs.size(); i++)
        {
            cmdString.append(cs.get(i)).append(" ");
        }

        logger.log("Starting execution of task " + currentTaskName + ".");
        logger.log("Command:   " + cmdString.toString());
        try
        {
            Thread.sleep(1000);
        }
        catch (InterruptedException ex)
        {
            java.util.logging.Logger.getLogger(TaskExecutor.class.getName()).log(Level.SEVERE, null, ex);
        }
        return pb.start();
    }

    private void prepareDirectory(Message msg)
    {
        WorkflowFile[] inputs = (WorkflowFile[]) msg.getObject("input_files");
        WorkflowFile[] outputs = (WorkflowFile[]) msg.getObject("output_files");
        for (WorkflowFile f : inputs)
        {
            File file = new File(currentWorkingDir + f.getName());
//            if (f.getType() == WorkflowFile.TYPE_DIRECTIORY)
//            {
//                file.mkdirs();
//            }
//            else
            {
                file.getParentFile().mkdirs();
            }
        }
        for (WorkflowFile f : outputs)
        {
            File file = new File(currentWorkingDir + f.getName());
//            if (f.getType() == WorkflowFile.TYPE_DIRECTIORY)
//            {
//                file.mkdirs();
//            }
//            else
            {
                file.getParentFile().mkdirs();
            }
        }
    }

    private String[] prepareExecutionAndGetCommands(Message msg) throws FileTransferException
    {
        currentWorkingDir = Utils.getProp("working_dir") + msg.get("wfid") + "/";
        currentRequestMsg = msg.copy();
        currentTaskName = msg.get("task_name");
        currentTaskDbid = msg.getInt("tid");
        String[] cmds = prepareCmd(msg);
        prepareDirectory(msg);
        downloadInputFiles(msg);
        downloadExecutable(msg);
        Utils.setExecutable(currentWorkingDir + currentProcName);
        return cmds;
    }

    synchronized public void exec(Message msg)
    {
        try
        {
            //IF the worker is busy
            if (status == STATUS_BUSY)
            {
                comm.sendResponseMsg(espAddr, msg, new Message(Message.TYPE_RESPONSE_TO_MANAGER)
                        .set("complete", false));
                return;
            }


            //////////////////////////START EXECUTING//////////////////////////
//            comm.sendEmptyResponseMsg(espAddr, msg, Message.TYPE_RESPONSE_TO_MANAGER);
            comm.sendResponseMsg(espAddr, msg, new Message(Message.TYPE_RESPONSE_TO_MANAGER)
                    .set("complete", true));

            isSuspensed = false;
            status = STATUS_BUSY;
            String[] cmds = prepareExecutionAndGetCommands(msg);
            currentProcess = startProcess(cmds, msg);

            //Send task status to manager that the task is started
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.set("task_name", currentTaskName);
            response.set("tid", currentTaskDbid);
            response.set("wfid", msg.get("wfid"));
            response.set("status", Task.STATUS_EXECUTING);
            response.set("start", currentTaskStart);
            response.set("end", -1);
            response.set("exit_value", -1);
            comm.sendMessage(espAddr, response);

            //Wait for the process to complete
            int exitVal = currentProcess.waitFor();


            synchronized (SUSPEND_LOCK)
            {
                currentTaskEnd = Utils.time();
                if (isSuspensed)
                {
                    return;
                }

                logger.log("Execution of task " + currentTaskName + " is finished." + exitVal);


                //Send task status to manager that the task is finished
                response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
                response.set("task_name", currentTaskName);
                response.set("tid", currentTaskDbid);
                response.set("wfid", msg.get("wfid"));
                if (exitVal == 0)
                {
                    response.set("status", Task.STATUS_COMPLETED);
                }
                else
                {
                    response.set("status", Task.STATUS_FAIL);
                    logger.log("STDERR");
                    logger.logFileContent(currentWorkingDir + currentTaskName + ".stderr");
                    logger.log("------");
                    logger.log("STDOUT");
                    logger.logFileContent(currentWorkingDir + currentTaskName + ".stdout");
                    logger.log("------");
                }
                response.set("start", currentTaskStart);
                response.set("end", currentTaskEnd);
                response.set("exit_value", exitVal);

                if (exitVal == 0)
                {
                    logger.log("Uploading output file from task " + currentTaskName + "...");
                    uploadOutputFiles(msg);
                    logger.log("Done.");

                }
                setIdle();
                logger.log("Updating node status and wait for acknowledgment...");
                updateNodeStatus(true);
                logger.log("Done.");
                logger.log("Updating task status ...");
                comm.sendForResponseSync(espAddr, port ,response);
                logger.log("Done.");
            }
        }
        catch (IOException | InterruptedException | FileTransferException ex)
        {
            currentTaskEnd = Utils.time();
            String errorMsg = "Exception while " + currentTaskName + " is executing: " + ex.getMessage();
            logger.log(errorMsg, ex);
            Message response = new Message(Message.TYPE_UPDATE_TASK_STATUS);
            response.set("task_name", currentTaskName);
            response.set("tid", currentTaskDbid);
            response.set("wfid", msg.get("wfid"));
            response.set("status", Task.STATUS_FAIL);
            response.set("start", currentTaskStart);
            response.set("end", currentTaskEnd);
            response.set("exit_value", -1);
            response.set("error_msg", errorMsg);
            try
            {
                comm.sendMessage(espAddr, response);
            }
            catch (IOException ex1)
            {
                logger.log("Cannot send message to " + espAddr + ".", ex1);
            }
            setIdle();
            updateNodeStatus();
        }
    }

    public void suspendCurrentTask(Message msg)
    {
        synchronized (SUSPEND_LOCK)
        {
            logger.log("Suspend request is received.");
            if (currentTaskName == null || currentTaskName.isEmpty())
            {
                try
                {
                    comm.sendResponseMsg(espAddr, msg, new Message(Message.TYPE_RESPONSE_TO_MANAGER)
                            .set("complete", true)
                            .set("from_file", "TaskExecutor.java:543"));
                }
                catch (IOException ex)
                {
                    logger.log("Cannot send response message.", ex);
                }
                return;
            }
            try
            {
                Message reqExecMsg = currentRequestMsg;
                int wfid = reqExecMsg.getInt("wfid");
                String workingDir = currentWorkingDir;
                String procName = currentProcName;
                String taskName = currentTaskName;
                Process proc = currentProcess;
                int tid = currentTaskDbid;
                long taskStart = currentTaskStart;
                long taskEnd = currentTaskEnd;

                boolean complete = true;

                logger.log("Checkpointing current task...");

                ProcessBuilder pb = Utils.createProcessBuilder(new String[]
                {
                    "/bin/bash", "-c", "dmtcp_command -p " + COOR_PORT + " --quiet -bc"
                }, workingDir, workingDir + "ckpt.out", workingDir + "ckpt.err", null);

                if (proc != null && !Utils.isProcTerminated(proc) && Utils.waitFor(pb.start(), 2))
                {
                    isSuspensed = true;

                    long ckptTime = Utils.time();
                    logger.log("Done.");


                    //Killing the current task
                    logger.log("Killing current task...");
                    Utils.createProcessBuilder(new String[]
                    {
                        "/bin/bash", "-c", "dmtcp_command -p " + COOR_PORT + " --quiet -k"
                    }, workingDir, workingDir + "kill.out", workingDir + "kill.err", null).start().waitFor();
                    logger.log("Done.");




                    //Update task status
                    Message taskStatusMsg = new Message(Message.TYPE_UPDATE_TASK_STATUS)
                            .set("task_name", taskName)
                            .set("tid", tid)
                            .set("wfid", wfid)
                            .set("status", Task.STATUS_SUSPENDED)
                            .set("start", taskStart)
                            .set("end", taskEnd)
                            .set("exit_value", -1);
                    comm.sendMessage(espAddr, taskStatusMsg);

                    //Uploading current output files
                    logger.log("Sending checkpoint-related files of the task...");
                    String ckptDir = reqExecMsg.get("file_dir");
                    WorkflowFile[] outputs = (WorkflowFile[]) reqExecMsg.getObject("output_files");
                    LinkedList<String> ckptInputs = new LinkedList<>();
                    for (WorkflowFile f : outputs)
                    {
                        logger.log("Uploading output files from checkpointing " + f.getName() + " from task " + taskName + "...");
                        String localFilePath = workingDir + f.getName();

                        try
                        {
                            if (Utils.isFileExist(localFilePath))
                            {
//                            if (Utils.isDir(localFilePath))
//                            {
//                                comm.sendDir(
//                                        espAddr.getHost(), 
//                                        espAddr.getPort(), 
//                                        ckptDir, 
//                                        localFilePath
//                                        );
//                            }
//                            else
                                {
                                    String filename = ckptDir + f.getName() + CKPT_OUTPUT_FILE_SUFFIX;
                                    comm.sendFile(
                                            espAddr.getHost(),
                                            espAddr.getPort(),
                                            filename,
                                            localFilePath);
                                    ckptInputs.add(filename);
                                }
                            }
                            logger.log("Done.");
                        }
                        catch (FileTransferException ex)
                        {
                            logger.log("Cannot upload output file " + f.getName() + ".", ex);
                            complete = false;
                        }
                    }

                    //Uploading STDOUT and STDERR files
                    String filename = "";
                    try
                    {
                        String stdoutFile = ckptDir + taskName + ".stdout";
                        if (Utils.isFileExist(stdoutFile))
                        {
                            filename = stdoutFile + CKPT_OUTPUT_FILE_SUFFIX;
                            comm.sendFile(
                                    espAddr.getHost(),
                                    espAddr.getPort(),
                                    filename,
                                    stdoutFile);
                            ckptInputs.add(filename);
                        }
                        String stderrFile = ckptDir + taskName + ".stderr";
                        if (Utils.isFileExist(stderrFile))
                        {
                            filename = stderrFile + CKPT_OUTPUT_FILE_SUFFIX;
                            comm.sendFile(
                                    espAddr.getHost(),
                                    espAddr.getPort(),
                                    filename,
                                    stderrFile);
                            ckptInputs.add(filename);
                        }
                    }
                    catch (FileTransferException ex)
                    {
                        logger.log("Cannot upload output file " + filename + ".", ex);
                        complete = false;
                    }



                    //Rename checkpointed file
                    String ckptFileName = taskName + "_" + ckptTime + ".dmtcp";
                    String ckptFilePath = workingDir + ckptFileName;
                    
//                    if(currentRequestMsg.getBoolean("migrate") == true)
//                    {
//                        Utils.createProcessBuilder(new String[]
//                        {
//                            "/bin/bash", "-c",
//                            "mv " + currentWorkingDir + currentRequestMsg.get("cmd") + " " + ckptFilePath
//                        }, workingDir, null, null, null).start().waitFor();
//                    }
//                    else
//                    {
                        String ori_ckpt = Utils.getLastModifiedFileInDir("/home/we/ckpt-files/"+tid);
                        Utils.createProcessBuilder(new String[]
                        {
                            "/bin/bash", "-c",
                            "cp " +ori_ckpt+" " + ckptFilePath
                        }, workingDir, null, null, null).start().waitFor();
//                    }
                    
                    //Upload checkpointed file
                    try
                    {
                        comm.sendFile(espAddr, ckptDir + ckptFileName, ckptFilePath);
                        new File(ckptFilePath).delete();
                    }
                    catch (FileTransferException ex)
                    {
                        logger.log("Cannot upload checkpoint file " + ckptFilePath + ".", ex);
                        complete = false;
                    }
                    logger.log("Done.");


                    //Register checkpointed files
                    Message registerMsg = new Message(Message.TYPE_REGISTER_CHECKPOINT_FILE)
                            .set("checkpoint_file_path", ckptDir + ckptFileName)
                            .set("name", wfid + "/" + tid + "/" + ckptFileName)
                            .set("tid", tid)
                            .set("wkid", wfid)
                            .set("time", ckptTime)
                            .set("ckpt_input_files", ckptInputs.toArray(new String[ckptInputs.size()]))
                            .set("comment", "TaskExecutor:758");
                    logger.log("Registering checkpoint ...");
                    comm.sendForResponseSync(espAddr, this.port, registerMsg);
                    logger.log("Done.");

                    //Reset and update worker status
                    logger.log("Update node status...");
                    setIdle();
                    updateNodeStatus(true);
                    logger.log("Done.");
                }
                //Response to manager
                Message response = new Message(Message.TYPE_RESPONSE_TO_MANAGER)
                        .set("complete", complete);
                comm.sendResponseMsg(espAddr, msg, response);
                logger.log("Finish checkpointing.");
            }
            catch (IOException | InterruptedException ex)
            {
                logger.log("Cannot suspend process ", ex);
            }
        }
    }
	
	public boolean readyForTask(Task t)
	{
		throw new UnsupportedOperationException("Not implemented");
	}
}
