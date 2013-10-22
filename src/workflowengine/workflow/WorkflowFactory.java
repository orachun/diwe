/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.workflow;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import workflowengine.utils.db.DBException;
import workflowengine.utils.Utils;
import workflowengine.utils.XMLUtils;

/**
 *
 * @author orachun
 */
public class WorkflowFactory
{

	public static final double AVG_WORKLOAD = 10;
	public static final double AVG_FILE_SIZE = 3 * Utils.MB;
	private static WorkflowFile[] montageExe = null;


	public static Workflow fromDAX(String filename, String name) throws DBException
	{
		HashMap<String, WorkflowFile> files = new HashMap<>();
		Workflow wf = new Workflow(name, Utils.uuid());
		try
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document dom = db.parse(filename);
			Element docEle = dom.getDocumentElement();
			String namespace = docEle.getAttribute("name");
			
			NodeList includedFileNodeList = docEle.getElementsByTagName("files");
			Set<WorkflowFile> includedFiles = new HashSet<>();
			if (includedFileNodeList != null && includedFileNodeList.getLength() > 0)
			{
				includedFileNodeList = ((Element)includedFileNodeList.item(0)).getElementsByTagName("file");
				for (int i = 0; i < includedFileNodeList.getLength(); i++)
				{
					Element e = (Element)includedFileNodeList.item(i);
					String fname = e.getAttribute("name");
					WorkflowFile wfile = files.get(fname);
					if (wfile == null)
					{
						wfile = new WorkflowFile(fname, 1, WorkflowFile.TYPE_EXEC, Utils.uuid());
						files.put(fname, wfile);
					}
					includedFiles.add(wfile);
				}
			}
			
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
//                    double runtime = Task.getRecordedExecTime(wf.getName(), idString+taskName);

					double runtime = -1;
					if (runtime == -1)
					{
						runtime = AVG_WORKLOAD;
					}

					String taskName = id + jobElement.getAttribute("name");
					Task task = new Task(taskName, "", runtime, Utils.uuid(), TaskStatus.waitingStatus(null));

					
					for (WorkflowFile f : includedFiles)
					{
						task.addInputFile(f);
					}
					


					tasks.put(id, task);

					NodeList fileNodeList = jobElement.getElementsByTagName("uses");
					for (int j = 0; j < fileNodeList.getLength(); j++)
					{
						Element fileElement = (Element) fileNodeList.item(j);
						String fname = fileElement.getAttribute("name");
						String fiotype = fileElement.getAttribute("link");
						char ftype = WorkflowFile.TYPE_FILE;
						double fsize = 1;
						WorkflowFile wfile = files.get(fname);
						if (wfile == null)
						{
							wfile = new WorkflowFile(fname, fsize, ftype, Utils.uuid());
							files.put(fname, wfile);
						}
						if (fiotype.equals("input"))
						{
							task.addInputFile(wfile);
						}
						else
						{
							task.addOutputFile(wfile);
						}
					}
					String cmd = XMLUtils.argumentTagToCmd(jobElement);

					//Set executable file as input file
					String execName = cmd.split(";")[0];
					WorkflowFile execFile = files.get(execName);
					if (execFile == null)
					{
						execFile = new WorkflowFile(execName, AVG_FILE_SIZE,
								WorkflowFile.TYPE_EXEC, Utils.uuid());
						files.put(execName, execFile);
					}
					task.addInputFile(execFile);

					task.setCmd(cmd);
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
							wf.taskGraph.addNodes(parent.getUUID(), child.getUUID());
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
		TaskRanker.rankTask(wf);
		return wf;
	}

	public static Workflow fromDummyDAX(String filename, String name)
	{
		HashMap<String, WorkflowFile> files = new HashMap<>();
		Workflow wf = new Workflow(name, Utils.uuid());
		WorkflowFile dummyFile = new WorkflowFile("dummy", 0.0088, WorkflowFile.TYPE_EXEC, Utils.uuid());
		try
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document dom = db.parse(filename);
			Element docEle = dom.getDocumentElement();
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

					String taskName = id + jobElement.getAttribute("name");
//                    int runtime = (int)Math.ceil(Double.parseDouble(jobElement.getAttribute("runtime")));
					int runtime = 3;
					String tid = Utils.uuid();
					Task task = new Task(taskName, "", runtime, tid, TaskStatus.waitingStatus(tid));

					StringBuilder cmdBuilder = new StringBuilder();
					cmdBuilder.append("dummy;").append(runtime).append(";");
					tasks.put(id, task);
					task.addInputFile(dummyFile);

					NodeList fileNodeList = jobElement.getElementsByTagName("uses");
					for (int j = 0; j < fileNodeList.getLength(); j++)
					{
						Element fileElement = (Element) fileNodeList.item(j);
						String fname = fileElement.getAttribute("file");
						String fiotype = fileElement.getAttribute("link");
						char ftype = WorkflowFile.TYPE_FILE;

						double fsize = 1 + Math.round(
								Double.parseDouble(fileElement.getAttribute("size")));

						WorkflowFile wfile = files.get(fname);
						if (wfile == null)
						{
							wfile = new WorkflowFile(fname, fsize, ftype, Utils.uuid());
							files.put(fname, wfile);
						}
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
						cmdBuilder.append((int) fsize).append(";");
					}
					cmdBuilder.deleteCharAt(cmdBuilder.length() - 1);
					task.setCmd(cmdBuilder.toString());
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
							wf.taskGraph.addNodes(parent.getUUID(), child.getUUID());
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
		TaskRanker.rankTask(wf);

		System.gc();
		return wf;
	}
}
