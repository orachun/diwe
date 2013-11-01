/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.schedule;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.util.HashMap;
import java.util.Map;
import workflowengine.utils.Utils;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.MongoDB;
import workflowengine.utils.db.Savable;

/**
 *
 * @author orachun
 */
public class ScheduleTable implements Savable
{
	private String uuid;
	private String superWfid;
	private String wfid;
	private HashMap<String, String> mapping; //Mapping from taskUUID to worker URI
    private HashMap<String, Double> estimatedStart;
    private HashMap<String, Double> estimatedFinish;
	private double cost = -1;
	private double makespan = -1;
	
	public ScheduleTable(String superWfid, String wfid, HashMap<String, String> mapping, HashMap<String, Double> estimatedStart, HashMap<String, Double> estimatedFinish)
	{
		this.uuid = "sch-"+superWfid;
		this.superWfid = superWfid;
		this.wfid = wfid;
		
		this.mapping = mapping == null? null : new HashMap<>(mapping);
		this.estimatedStart = estimatedStart == null? null : new HashMap<>(estimatedStart);
		this.estimatedFinish = estimatedFinish == null? null : new HashMap<>(estimatedFinish);
	}
	
	public void cache()
	{
		Cacher.cache(uuid, this);
	}
	
	public double getEstStartTime(String tid)
	{
		return estimatedStart.get(tid);
	}
	public double getEstFinishTime(String tid)
	{
		return estimatedFinish.get(tid);
	}
	public String getWorker(String tid)
	{
		return mapping.get(tid);
	}

	public String getUUID()
	{
		return uuid;
	}

	public String getWfid()
	{
		return wfid;
	}

	public String getSuperWfid()
	{
		return superWfid;
	}

	public double getCost()
	{
		return cost;
	}

	public void setCost(double cost)
	{
		this.cost = cost;
	}

	public double getMakespan()
	{
		return makespan;
	}

	public void setMakespan(double makespan)
	{
		this.makespan = makespan;
	}
	
		
	
	public static ScheduleTable get(String uuid)
	{
		return (ScheduleTable) Cacher.get(ScheduleTable.class, uuid);
	}
	
	public static ScheduleTable getSchduleForWorkflow(String superWfid)
	{
		return get("sch-"+superWfid);
	}

	public static ScheduleTable getInstance(Object key)
	{
		DBObject obj = MongoDB.SCHEDULE.findOne(new BasicDBObject("schid", (String)key));
		return parseDBObject(obj);
	}
	
	private static ScheduleTable parseDBObject(DBObject obj)
	{
		if(obj == null)
		{
			return null;
		}
		
		ScheduleTable scht = new ScheduleTable((String)obj.get("super_wfid"), 
				(String)obj.get("wfid"), null, null, null);
		HashMap<String, String> mapping = new HashMap<>();
		HashMap<String, Double> estStart = new HashMap<>();
		HashMap<String, Double> estFin = new HashMap<>();
		
		BasicDBList mappingList = (BasicDBList)obj.get("mapping");
		for(Object o : mappingList)
		{
			DBObject mappingObj = (DBObject)o;
			String tid = (String)mappingObj.get("tid");
			mapping.put(tid, (String)mappingObj.get("wkid"));
			estStart.put(tid, (Double)mappingObj.get("estimated_start"));
			estFin.put(tid, (Double)mappingObj.get("estimated_finish"));
		}
		scht.mapping = mapping;
		scht.estimatedStart = estStart;
		scht.estimatedFinish = estFin;
		scht.cost = (double)obj.get("cost");
		scht.makespan = (double)obj.get("makespan");
		return scht;
	}
	
	@Override
	public void save()
	{
		BasicDBObject obj = new BasicDBObject()
				.append("schid", uuid)
				.append("wfid", wfid)
				.append("super_wfid", superWfid)
				.append("cost", cost)
				.append("makespan", makespan);
		BasicDBList mappingList = new BasicDBList();
		
		for(Map.Entry<String, String>  entry : mapping.entrySet())
		{
			String tid = entry.getKey();
			String wkid = entry.getValue();
			
			BasicDBObject mappingObj = new BasicDBObject("tid", tid)
					.append("wkid", wkid)
					.append("estimated_start", estimatedStart.get(tid))
					.append("estimated_finish", estimatedFinish.get(tid));
			mappingList.add(mappingObj);
			
		}
		
		obj.append("mapping", mappingList);
		
		MongoDB.SCHEDULE.update(new BasicDBObject("wfid", wfid)
				.append("super_wfid", superWfid), 
					obj, true, false);
	}
	
	
}
