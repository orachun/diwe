/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author orachun
 */
public class UniqueMap<K,V> implements Map<K,V>
{
	private HashMap<K,V> keyMap = new HashMap<>();
	private HashMap<V,K> valueMap = new HashMap<>();
	
	public UniqueMap()
	{
		
	}
	
	public static <V> UniqueMap<Integer, V> fromArray(V[] array)
	{
		UniqueMap<Integer, V> map = new UniqueMap<>();
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

	public Object clone()
	{
		return keyMap.clone();
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

	
}
