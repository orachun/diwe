/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import removed.TaskManager;
import workflowengine.utils.DBException;
import workflowengine.utils.Utils;
import workflowengine.utils.XMLUtils;
import static workflowengine.workflow.Workflow.AVG_FILE_SIZE;
import static workflowengine.workflow.Workflow.AVG_WORKLOAD;

/**
 *
 * @author orachun
 */
public class WorkflowFactory
{
	public static Workflow fromDAX(String filename) throws DBException
    {
        File f = new File(filename);
        Workflow wf = new Workflow(f.getName());
        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(filename);
            Element docEle = dom.getDocumentElement();
            String namespace = docEle.getAttribute("name");
            NodeList jobNodeList = docEle.getElementsByTagName("job");
            
            HashMap<Integer, Task> tasks = new HashMap<>();
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element jobElement = (Element) jobNodeList.item(i);
                    
                    String idString = jobElement.getAttribute("id");
                    int id = Integer.parseInt(idString.substring(2));
//                    double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));//   

                    String taskName = jobElement.getAttribute("name");                 
                    double runtime = Task.getRecordedExecTime(wf.getName(), idString+taskName);
                    if(runtime == -1)
                    {
                        runtime = AVG_WORKLOAD;
                    }
//                    String taskNameSpace = jobElement.getAttribute("namespace");
                    Task task = Task.getWorkflowTask(idString+taskName, runtime, wf, "", namespace);
                    task.addInputFile(WorkflowFile.getFile(taskName, AVG_FILE_SIZE, WorkflowFile.TYPE_FILE));
                    StringBuilder cmdBuilder = new StringBuilder();
                    cmdBuilder.append("./dummy;").append(runtime).append(";");
                    tasks.put(id, task);

                    NodeList fileNodeList = jobElement.getElementsByTagName("uses");
                    for (int j = 0; j < fileNodeList.getLength(); j++)
                    {
                        Element fileElement = (Element) fileNodeList.item(j);
//                        String fname = fileElement.getAttribute("file");
                        String fname = fileElement.getAttribute("name");
                        String fiotype = fileElement.getAttribute("link");
//                        char ftype = fileElement.getAttribute("type").equals("dir") ? WorkflowFile.TYPE_DIRECTIORY:WorkflowFile.TYPE_FILE;
                        char ftype = WorkflowFile.TYPE_FILE;
//                        double fsize = Double.parseDouble(fileElement.getAttribute("size"));
                        double fsize = 1;
                        WorkflowFile wfile = WorkflowFile.getFile(fname, fsize, ftype);
                        if (fiotype.equals("input"))
                        {
                            cmdBuilder.append("i;");
                            task.addInputFile(wfile);
                        }
                        else
                        {
                            cmdBuilder.append("o;");
                            task.addOutputFile(wfile);
                        }
                        cmdBuilder.append(fname).append(";");
                        cmdBuilder.append(fsize).append(";");
                    }
                    cmdBuilder.deleteCharAt(cmdBuilder.length()-1);
                    
//                    String cmd = jobElement.getAttribute("cmd");
                    String cmd = XMLUtils.argumentTagToCmd(jobElement);
                    if(cmd == null)
                    {
                        task.setCmd(cmdBuilder.toString());
                    }
                    else
                    {
                        task.setCmd(cmd);
                    }
                    //wf.taskGraph.addNode(task);
                }
            }
            
            //Read dependencies
            jobNodeList = docEle.getElementsByTagName("child");
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element el = (Element) jobNodeList.item(i);
                    String refString = el.getAttribute("ref");
                    int childRef = Integer.parseInt(refString.substring(2));
                    Task child = tasks.get(childRef);
                    NodeList parents = el.getElementsByTagName("parent");
                    if (parents != null && parents.getLength() > 0)
                    {
                        for (int j = 0; j < parents.getLength(); j++)
                        {
                            el = (Element) parents.item(j);
                            String parentRefString = el.getAttribute("ref");
                            int parentRef = Integer.parseInt(parentRefString.substring(2));
                            Task parent = tasks.get(parentRef);
                            wf.taskGraph.addNodes(parent, child);
                        }
                    }
                }
            }
        }
        catch (ParserConfigurationException | SAXException | IOException | NumberFormatException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        
        wf.finalizeWorkflow();
        return wf;
    }
	
    public static Workflow fromDummyDAX(String filename, boolean forSim)
    {
        TaskManager.logger.log("Creating workflow "+filename+"...");
        File f = new File(filename);
        Workflow wf = new Workflow(f.getName());
        try
        {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(filename);
            Element docEle = dom.getDocumentElement();
            String namespace = "Dummy";
            NodeList jobNodeList = docEle.getElementsByTagName("job");

            TaskManager.logger.log("Reading workflow task...");
            HashMap<Integer, Task> tasks = new HashMap<>();
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element jobElement = (Element) jobNodeList.item(i);
                    
                    String idString = jobElement.getAttribute("id");
                    int id = Integer.parseInt(idString.substring(2));
//                    double runtime = Double.parseDouble(jobElement.getAttribute("runtime"));//   

                    String taskName = jobElement.getAttribute("name");      
                    int runtime = (int)Math.ceil(Double.parseDouble(jobElement.getAttribute("runtime"))/2);
                    Task task = Task.getWorkflowTask(idString+taskName, runtime, wf, "", namespace);
//                    task.addInputFile(WorkflowFile.getFile("dummy.sh", AVG_FILE_SIZE, WorkflowFile.TYPE_FILE));
                    StringBuilder cmdBuilder = new StringBuilder();
                    cmdBuilder.append("dummy.sh;").append(runtime).append(";");
                    tasks.put(id, task);

                    NodeList fileNodeList = jobElement.getElementsByTagName("uses");
                    for (int j = 0; j < fileNodeList.getLength(); j++)
                    {
                        Element fileElement = (Element) fileNodeList.item(j);
                        String fname = fileElement.getAttribute("file");
                        String fiotype = fileElement.getAttribute("link");
                        char ftype = WorkflowFile.TYPE_FILE;

                        double fsize = 1+Math.round(Double.parseDouble(fileElement.getAttribute("size"))*Utils.BYTE);
                        WorkflowFile wfile = WorkflowFile.getFile(fname, fsize, ftype);
                        if (fiotype.equals("input"))
                        {
                            cmdBuilder.append("i;");
                            task.addInputFile(wfile);
                        }
                        else
                        {
                            cmdBuilder.append("o;");
                            task.addOutputFile(wfile);
                        }
                        cmdBuilder.append(fname).append(";");
                        cmdBuilder.append((int)fsize).append(";");
                    }
                    cmdBuilder.deleteCharAt(cmdBuilder.length()-1);
                    task.setCmd(cmdBuilder.toString());
                }
            }

            TaskManager.logger.log("Reading workflow dependencies...");
            
            //Read dependencies
            jobNodeList = docEle.getElementsByTagName("child");
            if (jobNodeList != null && jobNodeList.getLength() > 0)
            {
                for (int i = 0; i < jobNodeList.getLength(); i++)
                {
                    Element el = (Element) jobNodeList.item(i);
                    String refString = el.getAttribute("ref");
                    int childRef = Integer.parseInt(refString.substring(2));
                    Task child = tasks.get(childRef);
                    NodeList parents = el.getElementsByTagName("parent");
                    if (parents != null && parents.getLength() > 0)
                    {
                        for (int j = 0; j < parents.getLength(); j++)
                        {
                            el = (Element) parents.item(j);
                            String parentRefString = el.getAttribute("ref");
                            int parentRef = Integer.parseInt(parentRefString.substring(2));
                            Task parent = tasks.get(parentRef);
                            wf.taskGraph.addNodes(parent, child);
                        }
                    }
                }
            }
        }
        catch (ParserConfigurationException | SAXException | IOException | NumberFormatException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
        
        wf.finalizeWorkflow();
        
        System.gc();
        return wf;
    }
	
	
	public static Workflow get(String uuid)
	{
		throw new UnsupportedOperationException("Method is not implemented.");
	}
}
