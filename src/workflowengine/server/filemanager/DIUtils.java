/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author orachun
 */
public class DIUtils
{
	public static void setPieceRequirements(DBCollection reqPcsColl, DBCollection fPrtColl, String fid, String worker, String wfid)
	{
		WorkflowFile file = WorkflowFile.get(fid);
		reqPcsColl.insert(new BasicDBObject()
				.append("worker", worker)
				.append("name", file.getName(wfid))
				.append("no", -1));
	}
	
	public static void setFilePriority(DBCollection fPrtColl, String fid, String wfid)
	{
		WorkflowFile file = WorkflowFile.get(fid);
		fPrtColl.update(
				new BasicDBObject("name", file.getName(wfid)),
				new BasicDBObject()
				.append("name", file.getName(wfid))
				.append("priority", file.getPriority()), true, false);
	}
}
