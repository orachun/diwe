/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author orachun
 */
public class A
{
    public static void main(String[] args) throws Exception
    {
		switch (args[0])
		{
			case "-p":
				setProp(args);
				break;
			case "-e":
				start();
				break;
		}
    }
	
	public static void start() throws Exception
	{
		final Process manager = Runtime.getRuntime().exec(new String[]{
			"java","-Xmx2g", "-cp", "/home/we/diwe/bin/diwe.jar", "workflowengine.server.SiteManager", "local_hostname=vm201", "local_port=9900", "managerhost=\"\"", "manager_port=\"\""
		});
		Thread.sleep(5000);
		Runtime.getRuntime().exec(new String[]{
			"bash", "-c", "/home/we/diwe/startallworkersubmit.sh"
		});
		
//		final BufferedReader managerOutput = new BufferedReader(
//				new InputStreamReader(manager.getInputStream()));
//		final BufferedReader managerError = new BufferedReader(
//				new InputStreamReader(manager.getErrorStream()));
//		
//		new Thread(){
//			@Override
//			public void run()
//			{
//				try
//				{
//					String line;
//					while ((line = managerOutput.readLine()) != null)
//					{
//						synchronized(manager)
//						{
//							System.out.println(line);
//						}
//					}
//				}
//				catch (IOException ex)
//				{
//					Logger.getLogger(A.class.getName()).log(Level.SEVERE, null, ex);
//				}
//			}
//			
//		}.start();
//		
//		new Thread(){
//			@Override
//			public void run()
//			{
//				try
//				{
//					String line;
//					while ((line = managerError.readLine()) != null)
//					{
//						synchronized(manager)
//						{
//							System.err.println(line);
//						}
//					}
//				}
//				catch (IOException ex)
//				{
//					Logger.getLogger(A.class.getName()).log(Level.SEVERE, null, ex);
//				}
//			}
//			
//		}.start();
		
		
		
		manager.waitFor();
		
		Thread.sleep(10000);
	}
	
	public static void setProp(String[] args) throws Exception
	{
		String filename = args[1];
		Properties p = new Properties();
		FileInputStream fis = new FileInputStream(filename); 
		p.load(fis);
		fis.close();
        for(int i=2;i<args.length;i++)
		{
			String[] propItem = args[i].split("=");
			p.setProperty(propItem[0], propItem[1]);
		}
		FileOutputStream fos = new FileOutputStream(filename);
		p.store(fos, "");
		fos.close();
	}
}
