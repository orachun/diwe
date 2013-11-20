/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.db;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;
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
	public static DBCollection EXEC_TIME;
	public static DBCollection WORKFLOW_SUBMIT_INFO;
	
	public static boolean prepare()
	{
		try
		{
			MongoClientOptions mOpt = MongoClientOptions.builder()
					.threadsAllowedToBlockForConnectionMultiplier(100)
					.build();
			MongoClient mClient = new MongoClient(new ServerAddress(
					Utils.getProp("DBHost"), 
					Utils.getIntProp("DBPort")), mOpt);
			String thisURI = Utils.getProp("local_hostname")+"_"+Utils.getProp("local_port");
			DB db = mClient.getDB(Utils.getProp("DBName")+"_"+thisURI.replace('.', '_'));
			db.dropDatabase();
			db = mClient.getDB(Utils.getProp("DBName")+"_"+thisURI.replace('.', '_'));
			
			WORKFLOW = db.getCollection("workflow");
			TASK = db.getCollection("task");
			WF_FILE = db.getCollection("wf_file");
			SCHEDULE = db.getCollection("schedule");
			
			
			db = mClient.getDB(Utils.getProp("DBName")+"_"+thisURI.replace('.', '_')+"_stats");
			EXEC_TIME = db.getCollection("execution_time");
			WORKFLOW_SUBMIT_INFO = db.getCollection("workflow_submit_info");
			
			
			return true;
		}
		catch (UnknownHostException ex)
		{
			Logger.getLogger(DifsysFile.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}
}
