/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * @author orachun
 */
public class LockStorage
{
	public static final String FILE_WAIT_LOCK = "FILE_WAIT_LOCK";
	
	
	
	private static Map<String, Map<Object, Object>> objectMap = new ConcurrentHashMap<>();
	private static Map<String, Map<Object, Lock>> lockMap = new ConcurrentHashMap<>();
	
	/**
	 * Get an object used for synchronized block
	 * @param group
	 * @param key
	 * @return 
	 */
	public static Object getObject(String group, Object key)
	{
		Map m = objectMap.get(group);
		if(m == null)
		{
			m = new ConcurrentHashMap<>();
			objectMap.put(group, m);
		}
		Object l = m.get(key);
		if(l == null)
		{
			l = new Object();
			m.put(key, l);
		}
		return l;
	}
	
	public static void remove(String group, Object key)
	{
		Map m = objectMap.get(group);
		if(m == null)
		{
			return;
		}
		m.remove(key);
	}
	
	/**
	 * Get a lock object
	 * @param group
	 * @param key
	 * @return 
	 */
	public static Lock getLock(String group, Object key)
	{
		Map<Object, Lock> m = lockMap.get(group);
		if(m == null)
		{
			m = new ConcurrentHashMap<>();
			lockMap.put(group, m);
		}
		Lock l = m.get(key);
		if(l == null)
		{
			l = new ReentrantLock(true);
			m.put(key, l);
		}
		return l;
	}
	
	public static void unlock(Lock l)
	{
		l.unlock();
	}
	
	public static void removeLock(String group, Object key)
	{
		Map m = lockMap.get(group);
		if(m == null)
		{
			return;
		}
		m.remove(key);
	}
}
