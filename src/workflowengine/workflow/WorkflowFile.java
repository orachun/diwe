/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.io.Serializable;
import java.util.Objects;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.MongoDB;
import workflowengine.utils.db.Savable;

/**
 *
 * @author Orachun
 */
public class WorkflowFile implements Serializable, Savable
{
    public static final char TYPE_FILE = 'F';
    public static final char TYPE_EXEC = 'E';
    public static final char TYPE_CHECKPOINT_FILE = 'C';
	
	private double priority = 0;
    private double sizeInBytes = 0;//Bytes
    private String name = "";
    private String uuid;
    private char type;
	public WorkflowFile(String name, double sizeInBytes, char type, String uuid)
	{
		this.sizeInBytes = sizeInBytes;
        this.name = name.trim();
        this.type = type;
		this.uuid = uuid;
		Cacher.cache(uuid, this);
	}

    public char getType()
    {
        return type;
    }
    
    public double getSize()
    {
        return sizeInBytes;
    }
	
	public void setSize(double s)
	{
		sizeInBytes = s;
	}

    public String getName()
    {
        return name;
    }
    public String getName(String wfid)
    {
        return wfid+"/"+name;
    }

	public String getUUID()
	{
		return uuid;
	}

	public double getPriority()
	{
		return priority;
	}

	public void setPriority(double priority)
	{
		this.priority = priority;
	}


    @Override
    public String toString()
    {
        return "["+type+"]"+name+"("+uuid+"):"+sizeInBytes+"Bytes";
    }

    @Override
    public boolean equals(Object obj)
    {
        if(!(obj instanceof WorkflowFile))
        {
            return false;
        }
        WorkflowFile f = (WorkflowFile)obj;
        return this.uuid.equals(f.uuid);
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.name);
        return hash;
    }
    
	
	
	
	
	
	
	
	
	
	
	
	public static WorkflowFile get(String UUID)
	{
		return (WorkflowFile)Cacher.get(WorkflowFile.class, UUID);
	}
	
    public static WorkflowFile getInstance(Object key)
	{
		DBObject obj = MongoDB.WF_FILE.findOne(new BasicDBObject("fid", key.toString()));
		if(obj == null)
		{
			return null;
		}
		WorkflowFile f = new WorkflowFile(
				(String)obj.get("name"),
				(double)obj.get("estsize"),
				((String)obj.get("file_type")).charAt(0),
				(String)obj.get("fid")
				);
		f.priority = (double)obj.get("priority");
		return f;
	}
	
	@Override
	public void save()
	{
		BasicDBObject obj = new BasicDBObject()
				.append("fid", uuid)
				.append("name", name)
				.append("estsize", sizeInBytes)
				.append("priority", priority)
				.append("file_type", String.valueOf(type));
		MongoDB.WF_FILE.update(new BasicDBObject("fid", uuid), obj, true, false);
	}
}
