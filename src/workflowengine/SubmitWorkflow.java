/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.util.Properties;
import workflowengine.server.WorkflowExecutor;
import workflowengine.utils.Utils;
import workflowengine.workflow.Workflow;
import workflowengine.workflow.WorkflowFactory;

/**
 *
 * @author udomo
 */
public class SubmitWorkflow
{
    public static void usage()
    {
        System.out.println("Usage: SubmitWorkflow DAX_FILE [option=value] ...");
    }
    
    public static void main(String[] args) throws IOException
    {
        if(args.length < 1)
        {
            System.err.println("Please specify DAG file.");
            usage();
            System.exit(1);
        }
        String daxFile = args[0];
        
        Properties p = new Properties();
        
		if (args.length > 1)
		{
			if (args[1].contains("="))
			{
				for (int i = 1; i < args.length; i++)
				{
					String[] prop = args[i].split("=");
					p.setProperty(prop[0].trim(), prop[1].trim());
				}
			}
			else
			{
				p.load(new FileInputStream(args[2]));
			}
		}

		Utils.setProp(p);
		
		daxFile = "/drive-d/Dropbox/Work (1)/Workflow Thesis/ExampleDAGs/Inspiral_30.xml";
		//daxFile = "/drive-d/Dropbox/Work (1)/Workflow Thesis/ExampleDAGs/Simple_5.xml";
		String dax = Charset.forName("UTF-8").decode(ByteBuffer.wrap(Files.readAllBytes(Paths.get(daxFile)))).toString();
		
		try
		{
			WorkflowExecutor.getSiteManager().submit(dax, null);
		}
		catch (NotBoundException ex)
		{
			System.err.println("Site manager is not found.");
		}
    }
}
