/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.util.Set;
import workflowengine.server.WorkflowExecutor;
import workflowengine.utils.SFTPClient;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class ServerClientFileManager extends FileManager
{
	private static ServerClientFileManager instant;
	
	public static FileManager get()
	{
		if(instant == null)
		{
			instant = new ServerClientFileManager();
		}
		return instant;
	}
	
	/**
	 * Download file from manager if not exists
	 * 
	 *
	 * @param filename
	 */
	@Override
	public void waitForFile(String name)
	{
		String fullpath = Utils.getProp("working_dir") + "/" + name;
		if (!Utils.fileExists(fullpath))
		{
//			System.out.println("Downloading: "+fullpath);
//			System.out.println("  From: "+
//						WorkflowExecutor.getSiteManager().getWorkingDir() + "/" + name);
			String remoteWorkingDir = WorkflowExecutor.getSiteManager().getWorkingDir();
			SFTPClient.get(Utils.getProp("manager_host"),
					remoteWorkingDir + "/" + name,
					fullpath);			
		}
	}
	/**
	 * Report the manager that the output file is created and ready to be
	 * transferred
	 *
	 * @param wff
	 */
	@Override
	public void outputFilesCreated(Set<String> filenames)
	{
		for(String name : filenames)
		{
//			System.out.println("Uploading file "+name);
			String fullpath = Utils.getProp("working_dir") + "/" + name;
			String remoteWorkingDir = WorkflowExecutor.getSiteManager().getWorkingDir();
			SFTPClient.put(Utils.getProp("manager_host"),
					fullpath,
					remoteWorkingDir + "/" + name);
		}
	}
	
	@Override
	public void shutdown()
	{
		
	}

}
