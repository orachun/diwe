/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.util.Set;
import workflowengine.schedule.Schedule;

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
	public abstract void setSchedule(Schedule s);
	public abstract void workerJoined(String uri);
	public abstract void setPeerSet(String uri, Set<String> peers);
	public abstract void outputFileCreated(String fname);
}
