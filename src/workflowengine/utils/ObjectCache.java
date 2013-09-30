/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.db;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author orachun
 */
public class ObjectCache
{
	private static int maxCacheSize = 500;
	private static HashMap<Object, Object> cache = new HashMap<>();
	private static LinkedList keyQ = new LinkedList();
	public static void cache(Object key, Object obj)
	{
		cache.put(key, obj);
		keyQ.add(key);
		flush();
	}
	
	public static Object get(Class c, Object key)
	{
		Object obj = cache.get(key);
		if(obj == null)
		{
			try
			{
				obj = c.getMethod("getFromDB", Object.class).invoke(null, key);
			}
			catch (NoSuchMethodException | IllegalAccessException | SecurityException | IllegalArgumentException | InvocationTargetException ex)
			{
				Logger.getLogger(ObjectCache.class.getName()).log(Level.SEVERE, null, ex);
			}
			cache(key, obj);
		}
		return obj;
	}
	
	private static void flush()
	{
		while(keyQ.size()>maxCacheSize/2)
		{
			cache.remove(keyQ.poll());
		}
	}
}
