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
	
	public static void startCoordinator()
	{
		bash("dmtcp_coordinator --background "+Utils.getProp("DMTCP_port"), false);
		command("k");
	}
	
	public static void stopCoordinator()
	{
		command("q");
	}
	
	public static void command(String cmd)
	{
		bash("dmtcp_command --port "+Utils.getProp("DMTCP_port")+" "+cmd, false);
	}
	
	public static void checkpoint()
	{
		command("bc");
	}
	
	public static String getExecCmdPrefix(String taskDir, String tid)
	{
		Utils.mkdirs(taskDir+"/"+tid+"_ckpt_data");
		return "dmtcp_checkpoint;--quiet;--port;"+Utils.getProp("DMTCP_port")
				+";--ckptdir;"+getCkptDir(taskDir,tid)+";";
	}
	
	public static String getResumeCmd(String taskDir, String tid)
	{
		String ckptDir = getCkptDir(taskDir,tid);
		String dmtcpFile = Utils.getFileFromWildcard(ckptDir+"/ckpt*.dmtcp");
		return "dmtcp_restart;--quiet;--port;"+Utils.getProp("DMTCP_port")
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
		Utils.bash(cmd, false);
	}
	
	public static void unpack(String ckptFileName, String taskDir)
	{
		Utils.bash("tar -xzf "+ckptFileName+" -C "+taskDir, false);
	}
}
