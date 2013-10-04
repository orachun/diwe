/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.rmi.registry.LocateRegistry;
import workflowengine.server.WorkflowExecutorInterface;

/**
 *
 * @author orachun
 */
public class A
{
	public int a(int b, String c)
	{
		return 0;
	}
    public static void main(String[] args) throws Exception
    {
        System.out.println(A.class.getMethod("a", Integer.TYPE, String.class).toString());
    }
}
