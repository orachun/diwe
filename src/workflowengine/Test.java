/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 *
 * @author orachun
 */
public class Test
{
	public interface IF{};
	public static void main(String[] args)
	{
		IF proxy = (IF)Proxy.newProxyInstance(IF.class.getClassLoader(), new Class[]{IF.class}, new InvocationHandler() {

			@Override
			public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
			{
				return null;
			}
		});
		
	}
}
