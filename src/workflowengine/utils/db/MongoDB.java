/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.db;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import difsys.DifsysFile;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class MongoDB
{
	public static DBCollection WORKFLOW;
	public static DBCollection TASK;
	public static DBCollection WF_FILE;
	public static DBCollection SCHEDULE;
	
	public static boolean prepare()
	{
		try
		{
			String thisURI = Utils.getProp("local_hostname")+"_"+Utils.getProp("local_port");
			DB db = new Mongo(
					Utils.getProp("DBHost"), 
					Utils.getIntProp("DBPort"))
					.getDB(Utils.getProp("DBName")+"_"+thisURI.replace('.', '_'));
			db.dropDatabase();
			db = new Mongo(
					Utils.getProp("DBHost"), 
					Utils.getIntProp("DBPort"))
					.getDB(Utils.getProp("DBName")+"_"+thisURI.replace('.', '_'));
			
			WORKFLOW = db.getCollection("workflow");
			TASK = db.getCollection("task");
			WF_FILE = db.getCollection("wf_file");
			SCHEDULE = db.getCollection("schedule");
			
			return true;
		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}
}
