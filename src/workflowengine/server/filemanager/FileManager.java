/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.util.Set;

/**
 *
 * @author orachun
 */
public abstract class FileManager
{
	public static FileManager get()
	{
//		return DIFileManager.get();
		return ServerClientFileManager.get();
	}
	
	public abstract void waitForFile(String name);
	public abstract void outputFilesCreated(Set<String> fname);
	public abstract void shutdown();
	public abstract long getTransferredBytes();
}
