/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import java.io.File;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author udomo
 */
public class SFTPClient
{
    private ChannelSftp sftpChannel;
    private Session session = null;

    private SFTPClient(ChannelSftp sftpChannel, Session s)
    {
        this.sftpChannel = sftpChannel;
        this.session = s;
    }

    public void close()
    {
        sftpChannel.exit();
        session.disconnect();
    }

    public static SFTPClient getSFTPClient(String hostname) throws JSchException
    {
        return getSFTPClient(hostname, 22, Utils.getProp("ssh_user"), Utils.getProp("ssh_pass"));
    }
    
    public static SFTPClient getSFTPClient(String hostname, int port, String user, String pass) throws JSchException
    {
        JSch jsch = new JSch();
        Session session = null;
        session = jsch.getSession(user, hostname, port);
        session.setConfig("StrictHostKeyChecking", "no");
        session.setPassword(pass);
        session.connect();

        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp sftpChannel = (ChannelSftp) channel;
        return new SFTPClient(sftpChannel, session);
        
    }

    public void put(String src, String dst) throws SftpException
    {
        sftpChannel.put(src, dst);
    }

    public void get(String src, String dst) throws SftpException
    {
        new File(dst).getParentFile().mkdirs();
        sftpChannel.get(src, dst);
    }

	public static void put(String host, String user, String pass, String src, String dst)
	{
		try
		{
			getSFTPClient(host, 21, user, pass).put(src, dst);
		}
		catch (JSchException | SftpException ex)
		{
			Logger.getLogger(SFTPClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	
	private static HashMap<String, SFTPClient> clients = new HashMap<>();
	
	public static void get(String host, String src, String dst)
	{
		try
		{
			SFTPClient c = clients.get(host);
			if(c == null)
			{
				c = getSFTPClient(host, Utils.getIntProp("ssh_port"), Utils.getProp("ssh_user"), Utils.getProp("ssh_pass"));
			}
			c.get(src, dst);
		}
		catch (JSchException | SftpException ex)
		{
			Logger.getLogger(SFTPClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public static void put(String host, String src, String dst)
	{
		try
		{
			
			SFTPClient c = clients.get(host);
			if(c == null)
			{
				c = getSFTPClient(host, Utils.getIntProp("ssh_port"), Utils.getProp("ssh_user"), Utils.getProp("ssh_pass"));
			}
			c.put(src, dst);
		}
		catch (JSchException | SftpException ex)
		{
			Logger.getLogger(SFTPClient.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
}
