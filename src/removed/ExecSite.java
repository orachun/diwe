/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package removed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import workflowengine.communication.HostAddress;
import workflowengine.resource.NetworkLink;
import workflowengine.utils.Utils;
import workflowengine.workflow.WorkflowFile;

/**
 *
 * @author Orachun
 */
public class ExecSite
{
    public static final String PROP_TRANSFER_UCOST = "transfer_unit_cost_per_gb";
    public static final String PROP_EST_LINK_SPD = "estimated_link_speed";
    public static final String PROP_EST_LINK_LATENCY = "estimated_link_latency";

    private HashMap<String, NetworkLink> edges = new HashMap<>();
    private ArrayList<Worker> workers = new ArrayList<>();
    private double estLinkSpd;
    private double estLatency;
    private double transferUnitCost;
    
    public ExecSite()
    {
        Utils.setPropIfNotExist(PROP_TRANSFER_UCOST, "0.01");
        Utils.setPropIfNotExist(PROP_EST_LINK_SPD, "5");
        Utils.setPropIfNotExist(PROP_EST_LINK_LATENCY, "0.5");
        
        transferUnitCost = Utils.getDoubleProp(PROP_TRANSFER_UCOST)/Utils.GB;
        estLinkSpd = Utils.getDoubleProp(PROP_EST_LINK_SPD);
        estLatency = Utils.getDoubleProp(PROP_EST_LINK_LATENCY);
    }
    
    private Iterable<Worker> workerIterable = new Iterable<Worker>()
    {
        @Override
        public Iterator<Worker> iterator()
        {
            return workers.iterator();
        }
    };

    public void addWorker(Worker w)
    {
        workers.add(w);
    }

    public double getTransferTime(WorkflowFile file)
    {
        return estLatency + file.getSize() / estLinkSpd;
    }

    public double getTransferTime(WorkflowFile[] files)
    {
        double total = 0;
        for (WorkflowFile f : files)
        {
            total += getTransferTime(f);
        }
        return total;
    }

    public double getTransferCost(WorkflowFile file)
    {
        return transferUnitCost*file.getSize();
    }
    public double getTransferCost(WorkflowFile[] files)
    {
        double total = 0;
        for (WorkflowFile f : files)
        {
            total += f.getSize();
        }
        return transferUnitCost*total;
    }
    
    public void setStorageLinkSpeed(double spd)
    {
        this.estLinkSpd = spd;
    }

    public int getTotalWorkers()
    {
        return workers.size();
    }
    
    
    public static ExecSite random(int count)
    {
        ExecSite n = new ExecSite();
        n.setStorageLinkSpeed((7+Math.random()*3)*Utils.GB);
        Worker[] servers = new Worker[count];
        HostAddress addr = new HostAddress("randomhost", 0);
        for(int i=0;i<count;i++)
        {
            servers[i] = Worker.updateWorkerStatus(addr, addr, count,  100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3, Utils.uuid(), 0);
//            servers[i] = Worker.getWorker("Server-"+(i+1), 100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3);
            n.addWorker(servers[i]);
        }
        return n;
    }
    
    public static ExecSite generate(int count)
    {
        ExecSite n = new ExecSite();
        n.setStorageLinkSpeed((7+Math.random()*3)*Utils.GB);
        Worker[] servers = new Worker[count];
        for(int i=0;i<count;i++)
        {
            HostAddress addr = new HostAddress("host"+(i+1), 0);
            servers[i] = Worker.updateWorkerStatus(addr, addr, -1,  1, 1, 1, Utils.uuid(), 0);
//            servers[i] = Worker.getWorker("Server-"+(i+1), 100+Math.random()*50, 100*Utils.GB+Math.random()*50*Utils.GB, 10+Math.random()*3);
            n.addWorker(servers[i]);
        }
        return n;
    }

    public Worker getWorker(int i)
    {
        return workers.get(i);
    }

    public Iterable<Worker> getWorkerIterable()
    {
        return workerIterable;
    }

    public int getWorkerIndex(Worker w)
    {
        return workers.indexOf(w);
    }

    public void print()
    {
//        for (String s : edges.keySet())
//        {
//            System.out.println(s + ":" + edges.get(s).getSpeed());
//        }
    }
    
    
}
