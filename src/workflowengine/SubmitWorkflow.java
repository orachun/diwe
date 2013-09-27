/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import workflowengine.communication.Communicator;
import workflowengine.communication.message.Message;
import workflowengine.utils.Utils;

/**
 *
 * @author udomo
 */
public class SubmitWorkflow
{
    public static void usage()
    {
        System.out.println("Usage: SubmitWorkflow DAX_FILE INPUT_FILE_DIR [option=value] ...");
    }
    
    public static void main(String[] args) throws IOException
    {
        if(args.length < 2)
        {
            System.err.println("Please specify DAG file and input file directory.");
            usage();
            System.exit(1);
        }
        String daxFile = args[0];
        String inputDir = args[1];
        
        Properties p = new Properties();
        
        if(args[2].contains("="))
        {
            for(int i=2;i<args.length;i++)
            {
                String[] prop = args[i].split("=");
                p.setProperty(prop[0].trim(), prop[1].trim());
            }
        }
        else
        {
            p.load(new FileInputStream(args[2]));
        }
        
        Message msg = new Message(Message.TYPE_SUBMIT_WORKFLOW);
        msg.set("dax_file", daxFile);
        msg.set("input_dir", inputDir);
        msg.set("properties", p);
        
        String host = Utils.getProp("task_manager_host");
        int port = Utils.getIntProp("task_manager_port");
        new Communicator("Workflow Submitor").sendMessage(host, port, msg);
    }
}
