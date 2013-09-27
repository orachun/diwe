/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import removed.TaskExecutor;
import removed.TaskManager;
import removed.ExecutionSiteProxy;
import java.util.Properties;
import workflowengine.utils.Utils;

/**
 *
 * @author Orachun
 */
public class WorkflowEngine
{

    public static void main(String[] args) throws ClassNotFoundException
    {
        if (args.length == 0)
        {
            System.err.println("Please specify server type (TaskExecutor, TaskManager, ExecutionSiteProxy).");
            System.err.println("Usage WorkflowEngine TYPE [option1 value1] ...");
            System.exit(1);
        }
        Properties p = new Properties();
        for(int i=1;i<args.length;i+=2)
        {
            p.setProperty(args[i], args[i+1]);
        }
        Utils.setProp(p);
        switch (args[0])
        {
            case "TaskExecutor":
                Utils.setPropIfNotExist("log_file", "te.log");
                System.err.println("Starting task executor ...");
                TaskExecutor taskExecutor = TaskExecutor.startService();
                if (taskExecutor != null)
                {
                    System.err.println("Task executor started.");
                }
                else
                {
                    System.err.println("Cannot start task executor.");
                }
                break;
            case "TaskManager":
                Utils.setPropIfNotExist("log_file", "tm.log");
                System.err.println("Starting task manager ...");
                TaskManager taskManager = TaskManager.startService();
                if (taskManager != null)
                {
                    System.err.println("Task manager started.");
                }
                else
                {
                    System.err.println("Cannot start task manager.");
                }
                break;
            case "ExecutionSiteProxy":
                Utils.setPropIfNotExist("log_file", "esp.log");
                System.err.println("Starting execution site proxy ...");
                ExecutionSiteProxy esp = ExecutionSiteProxy.startService();
                if (esp != null)
                {
                    System.err.println("Execution site proxy started.");
                }
                else
                {
                    System.err.println("Cannot start task executor.");
                }
                break;
        }
    }
}
