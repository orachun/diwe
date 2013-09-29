/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package removed;

import workflowengine.workflow.Task;
import java.io.File;
import workflowengine.communication.FileTransferException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import workflowengine.communication.Communicator;
import workflowengine.communication.HostAddress;
import workflowengine.communication.message.Message;
import workflowengine.utils.db.DBRecord;
import workflowengine.utils.SynchronizedHashMap;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author udomo
 */
public class ExecutionSiteProxy extends Service
{

    public static workflowengine.utils.Logger logger = Utils.getLogger();
    private HostAddress managerAddr;
    private int esid;
    private static ExecutionSiteProxy esp = null;
    private SynchronizedHashMap<String, HostAddress> workerMap = new SynchronizedHashMap<>(); //<uuid, host address>
//    private SynchronizedHashMap<String, Message> workerHeartbeatMsg = new SynchronizedHashMap<>(); //<uuid, host address>
    private static final ConcurrentHashMap<Integer, Message> completedTaskMsg = new ConcurrentHashMap<>();
    
    @Override
    protected void prepareComm()
    {

        comm = new Communicator("Execution Site Proxy")
        {
            @Override
            public void handleMessage(Message msg)
            {
                switch (msg.getType())
                {
                    case Message.TYPE_UPDATE_NODE_STATUS:
                        updateNodeStatus(msg);
                        break;

                    //From manager to executor
                    case Message.TYPE_RESPONSE_TO_WORKER:
                    case Message.TYPE_DISPATCH_TASK:
                    case Message.TYPE_GET_NODE_STATUS:
                    case Message.TYPE_SUSPEND_TASK:
                        forwardMsgToWorker(msg);
                        break;
                        
                    case Message.TYPE_SHUTDOWN:
                        System.exit(0);
                        break;

                    //From executor to manager
//                case Message.TYPE_SUBMIT_WORKFLOW:
                    case Message.TYPE_SUSPEND_TASK_COMPLETE:
                    case Message.TYPE_RESPONSE_TO_MANAGER:
                        forwardMsgToManager(msg);
                        break;

                    //From manager to ESP
                    case Message.TYPE_FILE_UPLOAD_REQUEST:
                        uploadFile(msg);
                        break;
                    case Message.TYPE_CHECKPOINT_FILE_UPLOAD_REQUEST:
                        uploadCheckPointFile(msg);
                        break;

                    case Message.TYPE_REGISTER_CHECKPOINT_FILE:
                        registerCheckpointFiles(msg);
                        break;

                    case Message.TYPE_REGISTER_FILE:
                        registerFile(msg);
                        break;


                    case Message.TYPE_UPDATE_TASK_STATUS:
                        updateTaskStatus(msg);
                        break;

                    case Message.TYPE_NONE: 
                        break;
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

    private  void updateTaskStatus(Message msg)
    {
        char status = msg.getCharParam("status");
        
        Task.updateTaskStatus(
                msg.getInt("tid"),
                msg.getInt("start"),
                msg.getInt("end"),
                msg.getInt("exit_value"),
                status);
        if (status == Task.STATUS_COMPLETED || status == Task.STATUS_FAIL)
        {
            int wfid = msg.getInt("wfid");
            if (Workflow.isFinished(wfid))
            {
                Workflow.setFinishedTime(wfid, Utils.time());
            }
            requestDispatchTask(msg);
        }
        if(msg.needResponse())
        {
            try
            {
                comm.sendEmptyResponseMsg(new HostAddress(msg.get(Message.PARAM_FROM), msg.getInt(Message.PARAM_RESPONSE_PORT)), msg, Message.TYPE_RESPONSE_TO_WORKER);
            }
            catch (IOException ex)
            {
                logger.log("Cannot send response message to worker", ex);
            }
        }
        
        if (status == Task.STATUS_COMPLETED)
        {
            completedTaskMsg.put(msg.getInt("tid"), msg);
        }
    }
    
    

    private void requestDispatchTask(Message taskStatusMsg)
    {
        Message msg = new Message(Message.TYPE_DISPATCH_TASK_REQUEST);
        msg.addAllParamsFromMsg(taskStatusMsg);
        msg.set("bytes_transferred", comm.getTotalBytesTransferred());
        try
        {
            comm.sendMessage(managerAddr, msg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot send dispatch task request message.", ex);
        }
    }

    private void uploadCheckPointFile(Message msg)
    {
    }

    private void forwardMsgToWorker(Message msg)
    {
        HostAddress workerAddr = workerMap.get(msg.get(Message.PARAM_WORKER_UUID));

        try
        {
            comm.sendMessage(workerAddr, msg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot sent message to " + workerAddr + ": " + ex.getMessage(), ex);
        }
    }

    private HostAddress recordWorker(Message msgFromWorker)
    {
        String uuid = msgFromWorker.get(Message.PARAM_WORKER_UUID);
        HostAddress workerAddr = workerMap.get(uuid);
        if (workerAddr == null)
        {
            workerAddr = new HostAddress(
                    msgFromWorker.get(Message.PARAM_FROM),
                    msgFromWorker.getInt(Message.PARAM_WORKER_PORT));
            workerMap.put(uuid, workerAddr);
        }
        return workerAddr;
    }

    private void prepareMsgToManager(Message msg)
    {
//        String uuid = msg.get(Message.PARAM_WORKER_UUID);
//        HostAddress workerAddr = workerMap.get(uuid);
//        if (workerAddr == null) {
//            workerAddr = new HostAddress(msg.get(Message.PARAM_FROM), msg.getInt(Message.PARAM_WORKER_PORT));
//            workerMap.put(uuid, workerAddr);
//        }
        HostAddress workerAddr = recordWorker(msg);
        msg.set(Message.PARAM_WORKER_ADDRESS, workerAddr);
        msg.set(Message.PARAM_ESP_ADDRESS, addr);
    }

    private void forwardMsgToManager(Message msg)
    {
        prepareMsgToManager(msg);
        try
        {
            comm.sendMessage(managerAddr, msg);
        }
        catch (IOException ex)
        {
            logger.log("Cannot sent message to " + managerAddr + ": " + ex.getMessage(), ex);
        }
    }

    private ExecutionSiteProxy() throws IOException
    {
        prepareComm();
        addr = new HostAddress(Utils.getPROP(), "exec_site_proxy_host", "exec_site_proxy_port");
        managerAddr = new HostAddress(Utils.getPROP(), "task_manager_host", "task_manager_port");
        esid = new DBRecord("exec_site",
                "hostname", addr.getHost(),
                "port", addr.getPort()).insertIfNotExist();
        comm.setTemplateMsgParam(Message.PARAM_ESP_ADDRESS, addr);
        comm.setListeningPort(addr.getPort());
        comm.startServer();
        
        
        
        new Thread(new Runnable() {

            @Override
            public void run()
            {
                while (true)
                {
                    LinkedList<Integer> removing = new LinkedList<>();
                    for(int tid : completedTaskMsg.keySet())
                    {
                        if(Task.getTaskStatus(tid) == Task.STATUS_COMPLETED)
                        {
                            removing.add(tid);
                        }
                    }
                    for(int tid: removing)
                    {
                        completedTaskMsg.remove(tid);
                    }
                    for(int tid : completedTaskMsg.keySet())
                    {
                        Message msg = completedTaskMsg.get(tid);
                        msg.set(Message.PARAM_NEED_RESPONSE, false);
                        updateTaskStatus(msg);
                    }
                    try
                    {
                        Thread.sleep(10000);
                    }
                    catch (InterruptedException ex)
                    {
                        logger.log("Interrupted while sleeping.", ex);
                    }
                }
            }
        }, "SET_COMPLETE_TASK_THREAD").start();
    }

    public static ExecutionSiteProxy startService()
    {
        try
        {
            if (esp == null)
            {
                esp = new ExecutionSiteProxy();
                new Thread(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        while (true)
                        {
                            esp.sendHeartbeat();
                            try
                            {
                                Thread.sleep(10000);
                            }
                            catch (InterruptedException ex)
                            {
                                logger.log("Interrupted while sleeping.", ex);
                            }
                        }
                    }
                }, "Heartbeat_THREAD").start();
                
                
                
            }
            logger.log("Execution site proxy is started.");
            return esp;
        }
        catch (IOException ex)
        {
            logger.log("Cannot start execution site proxy: " + ex.getLocalizedMessage());
            return null;
        }
    }

    public void sendHeartbeat()
    {
//        try {
//            Message msg = new Message(Message.TYPE_UPDATE_NODE_STATUS);
//            Message[] msgs = workerHeartbeatMsg.values().toArray(new Message[]{});
//            for (Message m : msgs) {
//                prepareMsgToManager(m);
//            }
//            msg.set(Message.PARAM_WORKER_MSGS, msgs);
//            comm.sendMessage(managerAddr, msg);
//            workerHeartbeatMsg.clear();
//        }
//        catch (IOException ex) {
//            logger.log("Cannot send heartbeat messages.");
//        }
    }

    public void uploadFile(Message msg)
    {
        Message response = new Message(Message.TYPE_RESPONSE_TO_MANAGER);
        String filename = msg.get("filename");
        String dir = msg.get("dir");
        String filepath = dir + filename;
        String uploadTo = msg.get("upload_to");
        try
        {
            logger.log("Sending file " + filepath + " to " + uploadTo);
            comm.sendFile(uploadTo, addr.getPort(), filepath, filepath);
            logger.log("Done.");
            response.set("upload_complete", true);
            response.set("status", "complete");
            insertFile(msg.getInt("fid"), uploadTo);
        }
        catch (FileTransferException ex)
        {
            logger.log("Cannot upload file " + filepath + " to " + uploadTo + ".", ex);
            response.set("exception", ex);
            response.set("upload_complete", false);
        }
        
        try
        {
            comm.sendResponseMsg(managerAddr, msg, response);
        }
        catch (IOException ex)
        {
            logger.log("Cannot send message: " + ex.getMessage());
        }

    }

    public void registerCheckpointFiles(Message msg)
    {
        String filepath = msg.get("checkpoint_file_path");
        File f = new File(filepath);
        int tid = msg.getInt("tid");
        WorkflowFile wff = WorkflowFile.getFile(f.getName(), f.length() * Utils.BYTE, WorkflowFile.TYPE_CHECKPOINT_FILE);
        Task t = Task.getWorkflowTaskFromDB(tid);
        t.setCheckpointFile(wff);
        t.addInputFile(wff);
        insertFile(wff.getDbid());
        String[] ckptInputs = (String[])msg.getObject("ckpt_input_files");
        for(String ckptFileName : ckptInputs)
        {
            File ckptFile = new File(ckptFileName);
            WorkflowFile ckptWff = WorkflowFile.getFile(
                    ckptFile.getName(), 
                    ckptFile.length() * Utils.BYTE, 
                    WorkflowFile.TYPE_CHECKPOINT_FILE
                    );
            t.addInputFile(ckptWff);
            insertFile(ckptWff.getDbid());
        }
        
        try
        {
            comm.sendResponseMsg(msg.get(Message.PARAM_FROM), msg.getInt(Message.PARAM_RESPONSE_PORT), msg, new Message(Message.TYPE_NONE));
        }
        catch (IOException ex)
        {
            logger.log("Cannot send response message.", ex);
        }
    }

    public void updateNodeStatus(Message msg)
    {
        HostAddress workerAddr = recordWorker(msg);
        Worker.updateWorkerStatus(
                this.addr,
                workerAddr,
                msg.getInt("current_tid"),
                msg.getDoubleParam("free_memory"),
                msg.getDoubleParam("free_space"),
                msg.getDoubleParam("cpu"),
                msg.get(Message.PARAM_WORKER_UUID),
                msg.getInt("total_usage"));
        if (msg.getBoolean(Message.PARAM_NEED_RESPONSE))
        {
            try
            {
//                System.out.println("Response to node status request: "+msg.getUUID());
                logger.log("Sending response message to worker " + workerAddr);
//                Message response = new Message(Message.TYPE_RESPONSE_TO_WORKER);
//                response.setParamFromMsg(msg, Message.PARAM_WORKER_UUID);
                comm.sendEmptyResponseMsg(workerAddr, msg, Message.TYPE_NONE);
//                comm.sendResponseMsg(workerAddr.getHost(), msg.getInt(Message.PARAM_RESPONSE_PORT), msg, new Message());
                logger.log("Done.");
            }
            catch (IOException ex)
            {
                logger.log("Cannot send response message.", ex);
            }
        }
    }

    public void registerFile(Message msg)
    {
        if (msg.getBoolean("is_workflow_file"))
        {
            WorkflowFile[] wfiles = (WorkflowFile[]) msg.getObject("files");
            for (WorkflowFile f : wfiles)
            {
                insertFile(f.getDbid());
            }
        }
        else
        {
            String file = msg.get("name");
            double size = msg.getDoubleParam("size");
            char type = msg.getCharParam("type");
            WorkflowFile f = WorkflowFile.getFile(file, size, type);
            insertFile(f.getDbid());
        }
        
        if(msg.needResponse())
        {
            try
            {
//                comm.sendResponseMsg(
//                        msg.get(Message.PARAM_FROM), 
//                        msg.getInt(Message.PARAM_RESPONSE_PORT), 
//                        msg, 
//                        new Message(Message.TYPE_NONE)
//                        );
                comm.sendEmptyResponseMsg(new HostAddress(msg.get(Message.PARAM_FROM), msg.getInt(Message.PARAM_RESPONSE_PORT)), msg, Message.TYPE_RESPONSE_TO_MANAGER);
            }
            catch (IOException ex)
            {
                logger.log("Cannot send response message.", ex);
            }
        }
    }

    private void insertFile(int fid)
    {
        new DBRecord("exec_site_file",
                "esid", esid,
                "fid", fid).insertIfNotExist();
    }

    private void insertFile(int fid, String espHost)
    {
        int espID = DBRecord.select("SELECT esid FROM exec_site "
                + "WHERE hostname='" + espHost + "'").get(0).getInt("esid");
        new DBRecord("exec_site_file",
                "esid", espID,
                "fid", fid).insertIfNotExist();
    }
//    
//    private void broadcastToWorker(Message msg)
//    {
//        Message attachedMsg = (Message)msg.getObject(Message.PARAM_ATTACHED_MSG);
//        boolean needResponse = msg.getBoolean(Message.PARAM_NEED_RESPONSE);
//        List<DBRecord> rec = DBRecord.select("SELECT hostname, port FROM worker WHERE esid='"+esid+"'");
//        for(DBRecord r : rec)
//        {
//            try{
//            if(needResponse)
//            {
//                comm.sendForResponseAsync(r.get("hostname"), r.getInt("port"), this.addr.getPort(), attachedMsg);
//            }
//            else
//            {
//                comm.sendMessage(r.get("hostname"), r.getInt("port"), msg);
//            }
//            }
//            catch(IOException ex)
//            {
//                logger.log("Cannot send message to "+r.get("hostname"), ex);
//            }
//        }
//    }
}
