/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

/**
 *
 * @author orachun
 */
public class SystemStats
{
    private static String exec(String cmd)
    {
        try
        {
            Process p = Runtime.getRuntime().exec(new String[]{"bash", "-c", cmd});
            BufferedReader buff = new BufferedReader(new InputStreamReader(p.getInputStream()));
            return buff.readLine();
        }
        catch(Exception e)
        {
            return "Error";
        }
    }
    
    public static int memFree()
    {
        String memfree = exec("cat /proc/meminfo | grep MemFree");
        memfree = memfree.replace("MemFree:", "");
        memfree = memfree.replace("kB", "").trim();
        return Integer.parseInt(memfree);
    }
    public static int memTotal()
    {
        String memfree = exec("cat /proc/meminfo | grep MemFree");
        memfree = memfree.replace("MemTotal:", "");
        memfree = memfree.replace("kB", "").trim();
        return Integer.parseInt(memfree);
    }
    
    public static double cpuMHz()
    {
        String memfree = exec("cat /proc/cpuinfo | grep 'cpu MHz'");
        memfree = memfree.replace("cpu MHz", "").trim();
        memfree = memfree.replace(":", "").trim();
        return Double.parseDouble(memfree);
    }
    
    public static double cpuUtil()
    {
        String util = exec("top -bn1 -p0 | grep \"Cpu(s)\"");
        util = util.replace("%Cpu(s):", "").trim();
        util = util.replaceAll("^(\\d+\\.\\d+ ..,)", "").trim();
        util = util.replaceAll("^(\\d+\\.\\d+ ..,)", "").trim();
        util = util.replaceAll("^(\\d+\\.\\d+ ..,)", "").trim();
        util = util.replaceAll("id.*", "").trim();
        return Double.parseDouble(util);
    }
    public static int totalProcesses()
    {
        String count = exec("ps -e | wc -l");
        return Integer.parseInt(count);
    }
    
	public static void printStat()
	{
		System.out.println(getStat());
	}
	
	public static String getStat()
	{
		StringBuilder stat = new StringBuilder();
		stat.append("Available processors (cores): ").append(Runtime.getRuntime().availableProcessors());

		/* Total amount of free memory available to the JVM */
		stat.append("\nFree memory (bytes): ").append(Runtime.getRuntime().freeMemory());

		/* This will return Long.MAX_VALUE if there is no preset limit */
		long maxMemory = Runtime.getRuntime().maxMemory();
		/* Maximum amount of memory the JVM will attempt to use */
		stat.append("\nMaximum memory (bytes): ").append(maxMemory == Long.MAX_VALUE ? "no limit" : maxMemory);

		/* Total memory currently in use by the JVM */
		stat.append("\nTotal memory (bytes): ").append(Runtime.getRuntime().totalMemory());

		/* Get a list of all filesystem roots on this system */
		File[] roots = File.listRoots();

		/* For each filesystem root, print some info */
		for (File root : roots)
		{
			stat.append("\nFile system root: ").append(root.getAbsolutePath());
			stat.append("\nTotal space (bytes): ").append(root.getTotalSpace());
			stat.append("\nFree space (bytes): ").append(root.getFreeSpace());
			stat.append("\nUsable space (bytes): ").append(root.getUsableSpace());
		}
		
		return stat.toString();
	}
}
