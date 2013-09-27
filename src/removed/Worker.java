/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package removed;

import workflowengine.workflow.Task;
import java.util.HashMap;
import workflowengine.communication.HostAddress;
import workflowengine.utils.DBRecord;
import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class Worker
{
    public static final String PROP_UNIT_COST = "worker_unit_cost";
    
    private HostAddress addr;
    private HostAddress espAddr;
    private double cpu; //Operations per time unit: represent server's performance
    private double unitCost; //cost per time unit
    private double freeStorage; //Storage in MB
    private double freeMemory; //Storage in MB
    private int dbid;
    private int currentTaskDbid;
    private String uuid;
    private HashMap<String, Object> objProbs = new HashMap<>();

    private Worker(HostAddress addr, HostAddress epsAddr, double cpu, double unitCost,
            double freeStorage, double freeMemory, int dbid,
            int currentTaskDbid, String uuid)
    {
        Utils.setPropIfNotExist(PROP_UNIT_COST, "0.000022222");
        this.addr = addr;
        this.espAddr = epsAddr;
        this.cpu = 1;
        this.unitCost = Utils.getDoubleProp(PROP_UNIT_COST);
        this.freeStorage = freeStorage;
        this.freeMemory = freeMemory;
        this.dbid = dbid;
        this.currentTaskDbid = currentTaskDbid;
        this.uuid = uuid;
    }

    public static Worker getWorkerFromDB(String uuid)
    {
        if(!Utils.isDBEnabled())
        {
            return null;
        }
        try
        {
            DBRecord r = DBRecord.select("worker",
                    "SELECT * "
                    + "FROM worker "
                    + "WHERE uuid='" + uuid + "'").get(0);
            Worker w = new Worker(
                    new HostAddress(r.get("hostname"), r.getInt("port")),
                    new HostAddress(r.get("esp_hostname"), r.getInt("esp_port")),
                    r.getDouble("cpu"),
                    r.getDouble("unit_cost"),
                    r.getDouble("free_space"),
                    r.getInt("free_memory"),
                    r.getInt("wkid"),
                    r.getInt("current_tid"),
                    uuid);
            return w;
        }
        catch (IndexOutOfBoundsException | NullPointerException ex)
        {
            return null;
        }
    }

    public void insert()
    {
        if(!Utils.isDBEnabled())
        {
            return;
        }
        int esid = new DBRecord("exec_site", 
                "hostname", espAddr.getHost(),
                "port", espAddr.getPort()).insertIfNotExist();
        
        
        dbid = new DBRecord("worker",
                "hostname", addr.getHost(),
                "port", addr.getPort(),
                "esp_hostname", espAddr.getHost(),
                "esp_port", espAddr.getPort(),
                "cpu", cpu,
                "unit_cost", unitCost,
                "free_space", freeStorage,
                "free_memory", freeMemory,
                "current_tid", currentTaskDbid,
                "uuid", uuid,
                "updated", Utils.time(),
                "esid", esid
         ).insert();
        
    }

    public static Worker updateWorkerStatus(HostAddress espAddr, 
            HostAddress workerAddr, int currentTid, double freeMem, 
            double freeStorage, double cpu, String uuid, int totalUsage)
    {
//        if(!Utils.isDBEnabled())
//        {
//            throw new RuntimeException("Database is disabled.");
//        }
        Worker w = getWorkerFromDB(uuid);
        if (w == null)
        {
            w = new Worker(workerAddr, espAddr, cpu, -1, freeStorage, freeMem, -1, currentTid, uuid);
            w.insert();
        }
        else
        {
            new DBRecord("worker",
                    "current_tid", currentTid,
                    "updated", Utils.time(),
                    "free_memory", freeMem,
                    "free_space", freeStorage,
                    "cpu", cpu,
                    "total_usage", totalUsage
                    ).update(new DBRecord("worker",
                    "uuid", uuid));
        }
        return w;

    }

    public int getDbid()
    {
        return dbid;
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof Worker)
        {
            return this.uuid.equals(((Worker) o).uuid);
        }
        else
        {
            return false;
        }
    }

    public String getName()
    {
        return addr.getHost();
    }

    public double getOps()
    {
        return cpu;
    }

    public double getExecTime(Task t)
    {
        return t.getOperations() / cpu;
    }

    public double getUnitCost()
    {
        return unitCost;
    }

    public double getExecCost(Task t)
    {
        return unitCost * getExecTime(t);
    }

    public String getEdgeName(Worker s)
    {
        if (this.dbid < s.dbid)
        {
            return "(" + this.dbid + "," + s.dbid + ")";
        }
        else
        {
            return "(" + s.dbid + "," + this.dbid + ")";
        }
    }

    public void setProp(String name, Object o)
    {
        objProbs.put(name, o);
    }

    public Object getProp(String name)
    {
        return objProbs.get(name);
    }

    public Double getDoubleProp(String name)
    {
        return (Double) objProbs.get(name);
    }

    @Override
    public String toString()
    {
        return addr.getHost() + "(" + cpu + ")";
    }

    public HostAddress getEspAddr()
    {
        return espAddr;
    }
    
    
}
