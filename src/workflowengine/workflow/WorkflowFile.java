/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.Serializable;
import java.util.Objects;
import workflowengine.utils.DBException;
import workflowengine.utils.DBRecord;
import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class WorkflowFile implements Serializable
{
//    private static int count = 0;
//    private static ArrayList<WorkflowFile> files = new ArrayList<>();
//    private int id;
    public static final char TYPE_FILE = 'F';
//    public static final char TYPE_DIRECTIORY = 'D';
    public static final char TYPE_CHECKPOINT_FILE = 'C';
    private double size = 0;//MB
    private String name = "";
    private int dbid;
    private char type;
    private WorkflowFile(String name, double size, char type)
    {
        this.size = size;
        this.name = name.trim();
        this.type = type;
    }
    public static WorkflowFile getFile(String name, double size, char type) throws DBException
    {
        WorkflowFile f = null;
        if(Utils.isDBEnabled())
        {
            f = getFileFromDB(name);
        }
        if(f == null)
        {
            f = new WorkflowFile(name, size, type);
            f.dbid = new DBRecord("file", 
                    "name", name, 
                    "estsize", size, 
                    "file_type", type
                    ).insert();
        }
        return f;
    }
    
    public static WorkflowFile getFileFromDB(String name)
    {
        if(!Utils.isDBEnabled())
        {
            throw new RuntimeException("Database is disabled.");
        }
        WorkflowFile f;
        try
        {
            DBRecord res = DBRecord.select("file", new DBRecord("file", "name", name)).get(0);
            f = new WorkflowFile(res.get("name"), res.getDouble("estsize"), res.get("file_type").charAt(0));
            f.dbid = res.getInt("fid");
            return f;
        }
        catch(IndexOutOfBoundsException ex)
        {
            return null;
        }
    }
    
    public static WorkflowFile getFileFromDB(int dbid)
    {
        WorkflowFile f;
        try
        {
            DBRecord res = DBRecord.select("file", new DBRecord("file", "fid", dbid)).get(0);
            f = new WorkflowFile(res.get("name"), res.getDouble("estsize"), res.get("file_type").charAt(0));
            f.dbid = res.getInt("fid");
            return f;
        }
        catch(IndexOutOfBoundsException ex)
        {
            return null;
        }
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

    public int getDbid()
    {
        return dbid;
    }

    @Override
    public String toString()
    {
        return "["+type+"]"+name+"("+dbid+"):"+size+"MB";
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
    
    
}
