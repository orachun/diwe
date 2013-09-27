/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author orachun
 */
public class ObjectCache
{
	private static HashMap<Class, HashMap<Object, Object >> cache = new HashMap<>();
	public static void cache(Class c, Object key, Object obj)
	{
		HashMap<Object, Object > classMap = cache.get(c);
		if(classMap == null)
		{
			classMap = new HashMap<>();
			cache.put(c, classMap);
		}
		classMap.put(key, obj);
	}
	
	public static Object get(Class c, Object key)
	{
		Object obj = cache.get(c).get(key);
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
			cache(c, key, obj);
		}
		return obj;
	}
}
