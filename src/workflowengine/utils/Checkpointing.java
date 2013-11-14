/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.File;
import java.util.Set;
import static workflowengine.utils.Utils.bash;

/**
 *
 * @author orachun
 */
public class Checkpointing
{
	
	public static int startCoordinator(int port)
	{
		bash("dmtcp_coordinator --background "+port);
		return command("k", port);
	}
	
	public static int stopCoordinator(int port)
	{
		return command("q", port);
	}
	
	public static int command(String cmd, int port)
	{
		return bash("dmtcp_command --port "+port+" "+cmd);
	}
	
	public static int checkpoint(int port)
	{
		return command("bc", port);
	}
	
	public static String getExecCmdPrefix(String taskDir, String tid, int port)
	{
		Utils.mkdirs(taskDir+"/"+tid+"_ckpt_data");
		return "dmtcp_checkpoint;--quiet;--port;"+port
				+";--ckptdir;"+getCkptDir(taskDir,tid)+";";
	}
	
	public static String getResumeCmd(String taskDir, String tid, int port)
	{
		String ckptDir = getCkptDir(taskDir,tid);
		String dmtcpFile = Utils.getFileFromWildcard(ckptDir+"/ckpt*.dmtcp");
		return "dmtcp_restart;--quiet;--port;"+port
				+";"+dmtcpFile;
	}
	
	public static String getCkptDir(String taskDir, String tid)
	{
		return taskDir+"/"+tid+"_ckpt_data";
	}
	
	public static void pack(String ckptFilename, String ckptDir, Set<String> additionalFiles)
	{
		File dir = new File(ckptDir);
		String cmd = "tar -zcf " + ckptFilename + " -C " + dir.getParent()+ ' ' + dir.getName() ;
		for(String f : additionalFiles)
		{
			cmd += ' ' + new File(f).getName();
		}
		Utils.bash(cmd);
	}
	
	public static void unpack(String ckptFileName, String taskDir)
	{
		Utils.bash("tar -xzf "+ckptFileName+" -C "+taskDir);
	}
}
