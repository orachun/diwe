/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 *
 * @author orachun
 */
public class Threading
{
	private static ExecutorService exeSrv = Executors.newFixedThreadPool(5);
	
	public static Future submitTask(Runnable task)
	{
		return exeSrv.submit(task);
	}
}
