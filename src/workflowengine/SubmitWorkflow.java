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
        System.out.println("Usage: SubmitWorkflow SUBMIT_FILE");
    }
    
    public static void main(String[] args) throws IOException
    {
        if(args.length < 1)
        {
            System.err.println("Please specify the submit file.");
            usage();
            System.exit(1);
        }
        String submitFile = args[0];
        
        Properties p = new Properties();
        p.load(new FileInputStream(submitFile));
		Utils.setProp(p);
		String dax = getFileContent(p.getProperty("dax_file"));
		
		try
		{
			WorkflowExecutor.getSiteManager().submit(dax, p);
		}
		catch (NotBoundException ex)
		{
			System.err.println("Site manager is not found.");
			System.exit(1);
		}
    }
	
	private static String getFileContent(String file) throws IOException
	{
		return Charset.forName("UTF-8")
				.decode(ByteBuffer.wrap(
					Files.readAllBytes(Paths.get(file))
				)).toString();
	}
}
