/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author orachun
 */
public class UniqueMap<K,V> implements Map<K,V>
{
	private Map<K,V> keyMap;
	private Map<V,K> valueMap;
	
	public UniqueMap(boolean concurrent)
	{
		if(concurrent)
		{
			keyMap = new ConcurrentHashMap<>();
			valueMap = new ConcurrentHashMap<>();
		}
		else
		{
			keyMap = new HashMap<>();
			valueMap = new HashMap<>();
		}
	}
	
	public static <V> UniqueMap<Integer, V> fromArray(V[] array)
	{
		return fromArray(array, false);
	}
	public static <V> UniqueMap<Integer, V> fromArray(V[] array, boolean concurrent)
	{
		UniqueMap<Integer, V> map = new UniqueMap<>(concurrent);
		for(int i=0;i<array.length;i++)
		{
			map.put(i, array[i]);
		}
		return map;
	}
	
	
	@Override
	public int size()
	{
		return keyMap.size();
	}

	public boolean isEmpty()
	{
		return keyMap.isEmpty();
	}

	public V get(Object key)
	{
		return keyMap.get(key);
	}
	
	public K getKey(Object value)
	{
		return valueMap.get(value);
	}

	public boolean containsKey(Object key)
	{
		return keyMap.containsKey(key);
	}

	public V put(K key, V value)
	{
		V oldVal = keyMap.get(key);
		if(oldVal != null)
		{
			valueMap.remove(oldVal);
		}
		valueMap.put(value, key);
		return keyMap.put(key, value);
	}

	public void putAll(Map<? extends K, ? extends V> m)
	{
		keyMap.putAll(m);
	}

	public V remove(Object key)
	{
		V oldVal = keyMap.get(key);
		if(oldVal != null)
		{
			valueMap.remove(oldVal);
		}
		return keyMap.remove(key);
	}

	public void clear()
	{
		keyMap.clear();
	}

	public boolean containsValue(Object value)
	{
		return keyMap.containsValue(value);
	}

	public Set<K> keySet()
	{
		return keyMap.keySet();
	}

	public Collection<V> values()
	{
		return keyMap.values();
	}

	public Set<Entry<K, V>> entrySet()
	{
		return keyMap.entrySet();
	}
	
	public Set<V> valueSet()
	{
		return valueMap.keySet();
	}

	
}
