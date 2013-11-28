/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.util.Set;
import workflowengine.server.filemanager.difm.NewDIFM;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public abstract class FileManager
{
	public static FileManager get()
	{
		Utils.setPropIfNotExist("file_manager", "ServerClientFileManager");
		String fmName = Utils.getProp("file_manager");
		switch(fmName)
		{
			case "DIFileManager" : return DIFileManager.get();
			case "NewDIFM" : return NewDIFM.get();
			default: return ServerClientFileManager.get();
		}
	}
	
	public abstract void waitForFile(String name);
	public abstract void outputFilesCreated(String wfid, Set<String> fname);
	public abstract void shutdown();
	public abstract long getTransferredBytes();
}
