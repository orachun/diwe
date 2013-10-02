/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.Serializable;
import java.util.Objects;
import workflowengine.utils.db.Cacher;
import workflowengine.utils.db.DBRecord;
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
	
	
    private double size = 0;//MB
    private String name = "";
    private String uuid;
    private char type;
	public WorkflowFile(String name, double size, char type, String uuid)
	{
		this.size = size;
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
        return size;
    }

    public String getName()
    {
        return name;
    }

	public String getUUID()
	{
		return uuid;
	}


    @Override
    public String toString()
    {
        return "["+type+"]"+name+"("+uuid+"):"+size+"MB";
    }

    @Override
    public boolean equals(Object obj)
    {
        if(!(obj instanceof WorkflowFile))
        {
            return false;
        }
        WorkflowFile f = (WorkflowFile)obj;
        return this.name.equals(f.name);
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
		try
		{
			DBRecord record = DBRecord.select("file", new DBRecord().set("name", key.toString())).get(0);
			return new WorkflowFile(
					record.get("name"),
					record.getDouble("estsize"),
					record.get("file_type").charAt(0),
					record.get("fid")
					);
		}
		catch (IndexOutOfBoundsException e)
		{
			return null;
		}
	}
	
	private static final String[] fileKeys = new String[]{"fid"};
	@Override
	public void save()
	{
		new DBRecord("file")
				.set("fid", uuid)
				.set("name", name)
				.set("estsize", size)
				.set("file_type", String.valueOf(type))
				.upsert(fileKeys);
	}
}
