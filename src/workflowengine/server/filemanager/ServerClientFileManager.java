/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import com.mongodb.DBObject;
import java.util.List;
import java.util.Set;
import workflowengine.server.WorkflowExecutor;
import workflowengine.utils.SFTPClient;
import workflowengine.utils.Utils;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class ServerClientFileManager implements FileManagerInterface
{
	/**
	 * Download file from manager if not exists
	 * 
	 *
	 * @param filename
	 */
	public void waitForFile(WorkflowFile wff, String workflowDirName)
	{
		String fullpath = Utils.getProp("working_dir") + "/" + workflowDirName + "/" + wff.getName();
		if (!Utils.fileExists(fullpath))
		{
			String remoteWorkingDir = WorkflowExecutor.getSiteManager().getWorkingDir();
			SFTPClient.get(Utils.getProp("manager_host"),
					remoteWorkingDir + "/" + workflowDirName + "/" + wff.getName(),
					Utils.getProp("working_dir") + "/" + workflowDirName);
			if (wff.getType() == WorkflowFile.TYPE_EXEC)
			{
				Utils.setExecutable(fullpath);
			}
		}
	}
	/**
	 * Report the manager that the output file is created and ready to be
	 * transferred
	 *
	 * @param wff
	 */
	public void outputCreated(WorkflowFile wff, String workflowDirName)
	{
		String fullpath = Utils.getProp("working_dir") + "/" + workflowDirName + "/" + wff.getName();
		String remoteWorkingDir = WorkflowExecutor.getSiteManager().getWorkingDir();
		SFTPClient.put(Utils.getProp("manager_host"),
				fullpath,
				remoteWorkingDir + "/" + workflowDirName);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	

	@Override
	public void setAllRequiredPieces(String invoker, List<DBObject> list)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setFilePriority(List<DBObject> list)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public List<DBObject> getRetrievedPieces(String invoker)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void workerJoined(String uri)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void setPeerSet(Set<String> workers)
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
	
	
}
