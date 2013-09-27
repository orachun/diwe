/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.BufferedReader;
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
    public static int procCount()
    {
        String count = exec("ps -e | wc -l");
        return Integer.parseInt(count);
    }
    
}
