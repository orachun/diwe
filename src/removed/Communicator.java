/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.communication;

/**
 *
 * @author Orachun
 */
import workflowengine.communication.message.Message;
import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;

public class Communicator
{

    private String name;
    private int listeningPort = 0;
    private Message templateMsg = new Message();
    private ConcurrentHashMap<String, Message> waitingMsgs = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Message> responseMsgs = new ConcurrentHashMap<>();
    private final String commUUID = Utils.uuid();
    private static final String MSG_PARAM_FILE_TYPE = "#IS_FILE_MESSAGE#";
    private boolean isStopped = false;
    private long totalBytesTransferred = 0;

    public Communicator(String name)
    {
        this.name = name;
    }

    public void stop()
    {
        isStopped = true;
    }

    public void startServer() throws IOException
    {
        final Communicator comm = this;
        final ServerSocket server = new ServerSocket(listeningPort);
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                while (!isStopped)
                {
                    try
                    {
                        final Socket socket = server.accept();
                        new Thread(new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try
                                {
                                    Message msg = readMessage(socket);
                                    if (msg.getBoolean(MSG_PARAM_FILE_TYPE))
                                    {
                                        handleFileRequest(msg, socket);
                                    }
                                    else
                                    {
                                        socket.close();

                                        String res = msg.get(Message.PARAM_STATE);
                                        String senderUUID = msg.get(Message.PARAM_SOURCE_UUID);
										
                                        if (res != null && res.equals(Message.STATE_RESPONSE) && senderUUID.equals(commUUID))
                                        {
                                            String uuid = msg.get(Message.PARAM_RESPONSE_FOR_MSG_UUID);
                                            Message orgMsg = waitingMsgs.get(uuid);
                                            responseMsgs.put(uuid, msg);

                                            if (orgMsg == null)
                                            {
                                                System.err.println(msg);
                                                System.err.println(name);
                                            }

                                            synchronized (orgMsg)
                                            {
                                                orgMsg.notify();
                                            }
                                        }
                                        else
                                        {
                                            handleMessage(msg);
                                        }
                                    }
                                }
                                catch (IOException | ClassNotFoundException | FileTransferException ex)
                                {
                                    Logger.getLogger(Communicator.class.getName()).log(Level.SEVERE, null, ex);
                                }
                                System.gc();
                            }
                        }).start();
                    }
                    catch (IOException ex)
                    {
                        Logger.getLogger(Communicator.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        }, "COMM_LISTENING_THREAD").start();
    }

    public void handleMessage(Message msg)
    {
    }

    /**
     * Read a message object from the given socket
     *
     * @param socket
     * @return
     * @throws ClassNotFoundException
     * @throws IOException
     */
    private Message readMessage(Socket socket) throws ClassNotFoundException, IOException
    {
        synchronized (socket)
        {
            ObjectInputStream os = new ObjectInputStream(socket.getInputStream());
            Message msg = (Message) os.readObject();
            msg.set(Message.PARAM_FROM, socket.getInetAddress().getHostAddress());
            msg.set(Message.PARAM_FROM_PORT, socket.getPort());

            if (msg.getBoolean(Message.PARAM_PRINT_AFTER_RECEIVE))
            {
                System.err.println("-----" + name + "-----");
                System.err.println(msg);
                System.err.println("------------------");
            }
            return msg;
        }
    }

    private void prepareMsg(Message msg)
    {
        msg.addAllParamsFromMsg(templateMsg);
        msg.setParamIfNotExist(Message.PARAM_NEED_RESPONSE, false);
        msg.setParamIfNotExist(Message.PARAM_PRINT_BEFORE_SENT, false);
        msg.setParamIfNotExist(Message.PARAM_PRINT_AFTER_RECEIVE, false);
        msg.setParamIfNotExist(Message.PARAM_NEED_RESPONSE, false);
        msg.setParamIfNotExist(Message.PARAM_SOURCE_UUID, this.commUUID);
        msg.setParamIfNotExist(MSG_PARAM_FILE_TYPE, false);

    }

    public void sendMessage(String host, int port, Message msg) throws IOException
    {
        prepareMsg(msg);
        if (msg.getBoolean(Message.PARAM_PRINT_BEFORE_SENT))
        {
            System.err.println("-----" + name + "-----");
            System.err.println(msg);
            System.err.println("------------------");
        }
        Socket s = new Socket(host, port);
        ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
        os.writeObject(msg);
        os.close();
        s.close();
        System.gc();
    }

    public void sendMessage(HostAddress addr, Message msg) throws IOException
    {
        sendMessage(addr.getHost(), addr.getPort(), msg);
    }

    public void setListeningPort(int port)
    {
        this.listeningPort = port;
    }

    public int getListeningPort()
    {
        return listeningPort;
    }

    public void setTemplateMsgParam(String key, Object val)
    {
        this.templateMsg.set(key, val);
    }

    /**
     *
     * @param targetHost
     * @param targetPort
     * @param responsePort
     * @param msg
     * @param isSync block until the response message is returned or not
     * @return
     * @throws IOException
     */
    private Message sendForResponse(String targetHost, int targetPort, int responsePort, Message msg, boolean isSync) throws IOException
    {
//        String uuid = Utils.uuid(waitingMsgs.keySet());
//        String uuid = Utils.uuid();
        msg.set(Message.PARAM_NEED_RESPONSE, true);
//        msg.set(Message.PARAM_MSG_UUID, uuid);
        msg.set(Message.PARAM_STATE, Message.STATE_REQUEST);
        msg.set(Message.PARAM_RESPONSE_PORT, responsePort);
        //System.out.println(new Date()+"Sending request: "+msg.getUUID());
        waitingMsgs.put(msg.getUUID(), msg);
        sendMessage(targetHost, targetPort, msg);
//        waitingMsgs.put(uuid, msg);

        if (!isSync)
        {
            return null;
        }
        else
        {
            return getResponseMessage(msg, false);
        }
    }

    public Message sendForResponseSync(String targetHost, int targetPort, int responsePort, Message msg) throws IOException
    {
        return sendForResponse(targetHost, targetPort, responsePort, msg, true);
    }

    public Message sendForResponseSync(HostAddress target, int responsePort, Message msg) throws IOException, InterruptedException
    {
        return sendForResponse(target.getHost(), target.getPort(), responsePort, msg, true);
    }

    /**
     * Send message to the specified host and port and save response message to
     * be obtained later via getResponseMessage method Must not reuse msg for
     * other sendForResponse method
     *
     * @param targetHost
     * @param targetPort
     * @param responsePort port number to receive response message
     * @param msg
     * @throws IOException
     */
    public void sendForResponseAsync(String targetHost, int targetPort, int responsePort, Message msg) throws IOException
    {
        sendForResponse(targetHost, targetPort, responsePort, msg, false);
    }

    /**
     * Send message to the specified host and port and save response message to
     * be obtained later via getResponseMessage method Must not reuse msg for
     * other sendForResponse method
     *
     * @param target
     * @param responsePort port number to receive response message
     * @param msg
     * @throws IOException
     */
    public void sendForResponseAsync(HostAddress target, int responsePort, Message msg) throws IOException
    {
//        System.err.println(msg);
        sendForResponseAsync(target.getHost(), target.getPort(), responsePort, msg);
    }

    /**
     * Wait and get response message of sending from sendForResponseAsync method
     *
     * @param sentMsg request message to get the response for
     * @param checkExisting whether the method should check the existing of sent
     * message
     * @return
     */
    private Message getResponseMessage(Message sentMsg, boolean checkExisting)
    {
//        System.out.println("Getting response for "+sentMsg.getUUID());
//        String uuid = sentMsg.get(Message.PARAM_MSG_UUID);
        String uuid = sentMsg.getUUID();
        if (checkExisting && !waitingMsgs.containsKey(uuid))
        {
            return null;
        }
        
        //Wait if the response is not arrived yet
        while (!responseMsgs.containsKey(uuid))
        {
            synchronized (sentMsg)
            {
                try
                {
                    sentMsg.wait(10000);
                }
                catch (InterruptedException ex)
                {
                    
//                    Logger.getLogger(Communicator.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        
        Message respMsg = responseMsgs.get(uuid);
        responseMsgs.remove(uuid);
        waitingMsgs.remove(uuid);
        return respMsg;
    }

    /**
     * Wait and get response message of sending from sendForResponseAsync method
     *
     * @param sentMsg
     * @return response Message or null if the given message is not in the
     * waiting list
     * @throws InterruptedException
     */
    public Message getResponseMessage(Message sentMsg)
    {
        return getResponseMessage(sentMsg, true);
    }

    /**
     * Send response message for the given original message
     *
     * @param original
     * @param response
     * @throws IOException
     */
    public void sendResponseMsg(HostAddress to, Message original, Message response) throws IOException
    {
        sendResponseMsg(to.getHost(), to.getPort(), original, response);
    }

    /**
     * Send response message for the given original message
     *
     * @param original
     * @param response
     * @throws IOException
     */
    public void sendResponseMsg(String toHost, int toPort, Message original, Message response) throws IOException
    {
        response.setParamFromMsg(original, Message.PARAM_SOURCE_UUID);
        response.set(Message.PARAM_STATE, Message.STATE_RESPONSE);
//        response.set(Message.PARAM_RESPONSE_FOR_MSG_UUID, original.get(Message.PARAM_MSG_UUID));
        response.set(Message.PARAM_RESPONSE_FOR_MSG_UUID, original.getUUID());
        response.set(Message.PARAM_RESPONSE_PORT, original.get(Message.PARAM_RESPONSE_PORT));
        response.set("original", original);
        //System.out.println(new Date()+"Sending response: "+original.getUUID());
        sendMessage(toHost, toPort, response);
    }

    /**
     * Send empty response message as acknowledgment message
     *
     * @param original original message
     * @param msgType message type indicate whether it's to manager or worker.
     * Only use Message.TYPE_RESPONSE_TO_WORKER or
     * Message.TYPE_RESPONSE_TO_MANAGER
     * @throws IOException
     */
    public void sendEmptyResponseMsg(HostAddress to, Message original, short msgType) throws IOException
    {
        sendResponseMsg(to.getHost(), original.getInt(Message.PARAM_RESPONSE_PORT), original, new Message(msgType));
    }
    /////////////////////////////////////////////////////
    ////////   Handle file request
    /////////////////////////////////////////////////////
    private static final String MSG_PARAM_FILE_REQUEST_TYPE = "#MSG_PARAM_FILE_MSG_TYPE#";
    private static final String MSG_PARAM_FILE_PATH = "#MSG_PARAM_FILE_PATH#";
    private static final String MSG_FILE_MSG_TYPE_DOWNLOAD = "#MSG_FILE_MSG_TYPE_DOWNLOAD#";
    private static final String MSG_FILE_MSG_TYPE_UPLOAD = "#MSG_FILE_MSG_TYPE_UPLOAD#";
    private static final String MSG_FILE_MSG_TYPE_LIST = "#MSG_FILE_MSG_TYPE_LIST#";

    private void handleFileRequest(Message msg, Socket s) throws FileTransferException
    {
        String filePath = msg.get(MSG_PARAM_FILE_PATH);
        File file = new File(filePath);
        try
        {
            OutputStream sos = s.getOutputStream();
            InputStream sis = s.getInputStream();
            switch (msg.get(MSG_PARAM_FILE_REQUEST_TYPE))
            {
                case MSG_FILE_MSG_TYPE_DOWNLOAD:
                    InputStream is = new FileInputStream(filePath);
                    Utils.pipe(is, sos);
                    is.close();
                    break;
                case MSG_FILE_MSG_TYPE_UPLOAD:
                    file.getParentFile().mkdirs();
                    FileOutputStream fos = new FileOutputStream(file);
                    Utils.pipe(sis, fos);
                    fos.flush();
                    fos.getChannel().force(true);
                    try
                    {
                        fos.getFD().sync();
                    }
                    catch (SyncFailedException e)
                    {
                    }
                    fos.close();

                    break;
                case MSG_FILE_MSG_TYPE_LIST:
                    ObjectOutputStream oos = new ObjectOutputStream(sos);
                    oos.writeObject(Utils.getfileListInDir(filePath));
                    oos.close();
                    break;
            }
            sos.close();
            System.gc();
            totalBytesTransferred += file.length();
        }
        catch (IOException ex)
        {
            throw new FileTransferException(ex);
        }
    }

    private Socket sendFileMsg(String host, int port, Message msg) throws FileTransferException
    {
        prepareMsg(msg);
        if (msg.getBoolean(Message.PARAM_PRINT_BEFORE_SENT))
        {
            System.err.println("-----" + name + "-----");
            System.err.println(msg);
            System.err.println("------------------");
        }
        try
        {
            Socket s = new Socket(host, port);
            ObjectOutputStream os = new ObjectOutputStream(s.getOutputStream());
            os.writeObject(msg);
            return s;
        }
        catch (IOException ex)
        {
            throw new FileTransferException(ex);
        }
    }

    /**
     * Download a file defined by <i>remoteFilePath</i> into a file defined by
     * <i>localFilePath</i>
     *
     * @param host
     * @param port
     * @param remoteFilePath
     * @param localFilePath
     * @throws FileTransferException
     */
    public void getFile(String host, int port, String remoteFilePath, String localFilePath) throws FileTransferException
    {
//        try
//        {
//            SFTPClient c = SFTPClient.getSFTPClient(host);
//            c.get(remoteFilePath, localFilePath);
//        }
//        catch (JSchException | SftpException e)
//        {
//            throw new FileTransferException(e);
//        }

        Message msg = new Message(Message.TYPE_NONE);
        msg.set(MSG_PARAM_FILE_TYPE, Boolean.TRUE);
        msg.set(MSG_PARAM_FILE_REQUEST_TYPE, MSG_FILE_MSG_TYPE_DOWNLOAD);
        msg.set(MSG_PARAM_FILE_PATH, remoteFilePath);
        Socket s = sendFileMsg(host, port, msg);
        File f = new File(localFilePath);
        f.getParentFile().mkdirs();
        try
        {
            f.createNewFile();
            FileOutputStream fos = new FileOutputStream(localFilePath);
            InputStream is = s.getInputStream();
            Utils.pipe(is, fos);
            is.close();
            fos.getChannel().force(true);
            try
            {
                fos.getFD().sync();
            }
            catch (SyncFailedException e)
            {
            }
            fos.close();
            s.close();
        }
        catch (IOException ex)
        {
            throw new FileTransferException(ex);
        }
    }

    /**
     * Download a file named fileName from <i>fromRemoteDir</i> and save into
     * <i>toLocalDir</i> with the same name.
     *
     * @param host
     * @param port
     * @param fromRemoteDir
     * @param toLocalDir
     * @param fileName
     * @throws FileTransferException
     */
    public void getFile(String host, int port, String fromRemoteDir, String toLocalDir, String fileName) throws FileTransferException
    {
        getFile(host, port, fromRemoteDir + fileName, toLocalDir + fileName);
    }

    /**
     * Upload a file defined by <i>localFilePath</i> into a remote file defined
     * by <i>remoteFilePath</i>
     *
     * @param host
     * @param remoteFilePath
     * @param localFilePath
     * @throws FileTransferException
     */
    public void sendFile(HostAddress host, String remoteFilePath, String localFilePath) throws FileTransferException
    {
        sendFile(host.getHost(), host.getPort(), remoteFilePath, localFilePath);
    }

    /**
     * Upload a file defined by <i>localFilePath</i> into a remote file defined
     * by <i>remoteFilePath</i>
     *
     * @param host
     * @param port
     * @param remoteFilePath
     * @param localFilePath
     * @throws FileTransferException
     */
    public void sendFile(String host, int port, String remoteFilePath, String localFilePath) throws FileTransferException
    {
        Message msg = new Message(Message.TYPE_NONE);
        msg.set(MSG_PARAM_FILE_TYPE, Boolean.TRUE);
        msg.set(MSG_PARAM_FILE_REQUEST_TYPE, MSG_FILE_MSG_TYPE_UPLOAD);
        msg.set(MSG_PARAM_FILE_PATH, remoteFilePath);
        Socket s = sendFileMsg(host, port, msg);
        try
        {
            FileInputStream fis = new FileInputStream(localFilePath);
            OutputStream os = s.getOutputStream();
            Utils.pipe(fis, os);
            os.close();
            fis.close();
            s.close();
        }
        catch (IOException ex)
        {
            throw new FileTransferException(ex);
        }
    }

    /**
     * Get a list of all files in tree structure inside the remote directory
     * <i>inRemoteDir</i>
     *
     * @param host
     * @param port
     * @param inRemoteDir
     * @return
     * @throws FileTransferException
     */
    public String[] getFileList(String host, int port, String inRemoteDir) throws FileTransferException
    {
        Message msg = new Message(Message.TYPE_NONE);
        msg.set(MSG_PARAM_FILE_TYPE, Boolean.TRUE);
        msg.set(MSG_PARAM_FILE_REQUEST_TYPE, MSG_FILE_MSG_TYPE_LIST);
        msg.set(MSG_PARAM_FILE_PATH, inRemoteDir);
        Socket s = sendFileMsg(host, port, msg);
        try
        {
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            String[] dirTree = (String[]) ois.readObject();
            ois.close();
            s.close();
            return dirTree;
        }
        catch (IOException | ClassNotFoundException ex)
        {
            throw new FileTransferException(ex);
        }
    }

    /**
     * Download a directory <i>remoteDir</i> and save this directory inside
     * <i>toLocalDir</i>
     *
     * @param host
     * @param port
     * @param remoteDir
     * @param toLocalDir
     * @throws FileTransferException
     */
    public void getDir(String host, int port, String remoteDir, String toLocalDir) throws FileTransferException
    {
        String[] fileList = getFileList(host, port, remoteDir);
        String remoteParent = new File(remoteDir).getParent();

        for (String remoteFilePath : fileList)
        {
            String localPath = remoteFilePath.replace(remoteParent, toLocalDir);
            getFile(host, port, remoteFilePath, localPath);
        }
    }

    /**
     * Upload a directory <i>localDir</i> and save this directory inside
     * <i>toRemoteDir</i>
     *
     * @param host
     * @param port
     * @param toRemoteDir
     * @param localDir
     * @throws FileTransferException
     */
    public void sendDir(String host, int port, String toRemoteDir, String localDir) throws FileTransferException
    {
        String[] fileList = Utils.getfileListInDir(localDir);
        String localParent = new File(localDir).getParent();
        for (String filePath : fileList)
        {
            String remotePath = filePath.replace(localParent, toRemoteDir);
            sendFile(host, port, remotePath, filePath);
        }
    }

    /**
     * Upload contents (files and directories) in the <i>localDir</i> and save
     * inside <i>toRemoteDir</i>
     *
     * @param host
     * @param port
     * @param toRemoteDir
     * @param localDir
     * @throws FileTransferException
     */
    public void sendFilesInDir(String host, int port, String toRemoteDir, String localDir) throws FileTransferException
    {
        String[] fileList = Utils.getfileListInDir(localDir);
        for (String filePath : fileList)
        {
            String remotePath = filePath.replace(localDir, toRemoteDir);
            sendFile(host, port, remotePath, filePath);
        }
    }

    public void sendFile(String host, int port, String remoteFilePath, String localFilePath, long offset, long length) throws FileTransferException
    {
        Message msg = new Message(Message.TYPE_NONE);
        msg.set(MSG_PARAM_FILE_TYPE, Boolean.TRUE);
        msg.set(MSG_PARAM_FILE_REQUEST_TYPE, MSG_FILE_MSG_TYPE_UPLOAD);
        msg.set(MSG_PARAM_FILE_PATH, remoteFilePath);
        msg.set("offset", offset);
        msg.set("length", length);

        Socket s = sendFileMsg(host, port, msg);
        try
        {
            FileInputStream fis = new FileInputStream(localFilePath);
            OutputStream os = s.getOutputStream();
            Utils.pipe(fis, os);
            os.close();
            fis.close();
            s.close();
        }
        catch (IOException ex)
        {
            throw new FileTransferException(ex);
        }
    }

    public long getTotalBytesTransferred()
    {
        return totalBytesTransferred;
    }
    
    
}
