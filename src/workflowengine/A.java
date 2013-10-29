/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import workflowengine.utils.Utils;

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
        for(Object k : System.getenv().keySet())
		{
			System.out.println(k+":"+System.getenv(k.toString()));
		}
    }
}
