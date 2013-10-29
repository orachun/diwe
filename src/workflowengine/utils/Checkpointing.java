/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

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
	
	public static String getResumeCmdPrefix(String taskDir, String tid)
	{
		return "dmtcp_restart;--quiet;--port;"+Utils.getProp("DMTCP_port")
				+";--ckptdir;"+getCkptDir(taskDir,tid)+";";
	}
	
	public static String getCkptDir(String taskDir, String tid)
	{
		return taskDir+"/"+tid+"_ckpt_data";
	}
}
