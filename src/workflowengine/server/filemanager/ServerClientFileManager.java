/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import workflowengine.server.WorkflowExecutor;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class ServerClientFileManager extends FileManager
{
	private static ServerClientFileManager instant;
	private WorkflowExecutor thisSite;
	private long transferredBytes = 0;
	private FileServer fileServer;
	private Map<String, String> locks = new ConcurrentHashMap<>();
	
	private ServerClientFileManager()
	{
		thisSite = WorkflowExecutor.get();
		try
		{
			fileServer = FileServer.get(thisSite.getWorkingDir());
		}
		catch (IOException ex)
		{
			thisSite.logger.log("Cannot start file server."+ex.getMessage(), ex);
		}
		Utils.bash("rm -rf "+thisSite.getWorkingDir()+"/*");
	}
	
	
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
		
		String lock = locks.get(name);
		if(lock == null)
		{
			lock = name;
			locks.put(name, name);
		}
		synchronized(lock)
		{
			if(Utils.fileExists(fullpath))
			{
				return;
			}
			try
			{
				transferredBytes += FileServer.request(
						thisSite.getWorkingDir(),
						name, FileServer.TYPE_DOWNLOAD_REQ,
						thisSite.getManagerURI());
			}
			catch (IOException ex)
			{
				thisSite.logger.log("Cannot download file.", ex);
			}
			locks.remove(lock);
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
			try
			{
				transferredBytes += FileServer.request(
						thisSite.getWorkingDir(),
						name, 
						FileServer.TYPE_UPLOAD_REQ,
						thisSite.getManagerURI());
			}
			catch (IOException ex)
			{
				thisSite.logger.log("Cannot upload file.", ex);
			}
		}
	}
	
	public int getPort()
	{
		return fileServer.getPort();
	}
	
	@Override
	public void shutdown()
	{
		
	}

	@Override
	public long getTransferredBytes()
	{
		return transferredBytes;
	}

	
	
}
