/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author orachun
 */
public class LockStorage
{
	public static final String FILE_WAIT_LOCK = "FILE_WAIT_LOCK";
	
	
	
	private static Map<String, Map<Object, Object>> lockMap = new ConcurrentHashMap<>();
	
	public static Object get(String group, Object key)
	{
		Map m = lockMap.get(group);
		if(m == null)
		{
			m = new ConcurrentHashMap<>();
			lockMap.put(group, m);
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
		Map m = lockMap.get(group);
		if(m == null)
		{
			return;
		}
		m.remove(key);
	}
}
