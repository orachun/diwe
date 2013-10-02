/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.db;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author orachun
 */
public class Cacher
{
	private static int maxCacheSize = 5500;
	private static HashMap<Object, Savable> cache = new HashMap<>();
	private static LinkedList keyQ = new LinkedList();
	public static void cache(Object key, Savable obj)
	{
		if(key != null && obj != null)
		{
			cache.put(key, obj);
			keyQ.add(key);
			flush();
		}
	}
	
	/**
	 * Get the object from the given key from the cache without looking
	 * from the DB.
	 * @param key
	 * @return Object from the cache or null if no object in the cache.
	 */
	public static Savable get(Object key)
	{
		return cache.get(key);
	}
	
	/**
	 * Get the object from the given key. It starts search from the cache.
	 * If the object is not in the cache, it is grabbed from DB.
	 * @param c
	 * @param key
	 * @return 
	 */
	public static Savable get(Class c, Object key)
	{
		Savable obj = cache.get(key);
		if(obj == null)
		{
			try
			{
				obj = (Savable)c.getMethod("getInstance", Object.class).invoke(null, key);
			}
			catch (NoSuchMethodException | IllegalAccessException | SecurityException | IllegalArgumentException | InvocationTargetException ex)
			{
				Logger.getLogger(Cacher.class.getName()).log(Level.SEVERE, null, ex);
			}
			cache(key, obj);
		}
		return obj;
	}
	
	private static void flush()
	{
		while(keyQ.size()>maxCacheSize/2)
		{
			Object obj = keyQ.poll();
			cache.remove(obj).save();
		}
	}
	
	public static void flushAll()
	{
		while(!keyQ.isEmpty())
		{
			cache.remove(keyQ.poll()).save();
		}
	}
	
	public static void saveAll()
	{
		for(Savable obj : cache.values())
		{
			obj.save();
		}
	}
	
	public Set<?> getAllInstances(Class<?> c)
	{
		Set s = new HashSet<>();
		for(Savable obj : cache.values())
		{
			try
			{
				s.add(c.cast(obj));
			}
			catch(ClassCastException e){}
		}
		return s;
	}
}
