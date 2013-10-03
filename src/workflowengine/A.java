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
    public static void main(String[] args) throws Exception
    {
        for(String s : LocateRegistry.getRegistry("10.217.168.205").list())
		{
			((WorkflowExecutorInterface)LocateRegistry.getRegistry("10.217.168.205").lookup(s)).greeting("hello");
			System.out.println(s);
		}
    }
}
