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
 * HashMap with all methods synchronized
 * @author udomo
 */
public class SynchronizedHashMap<K, V> extends HashMap<K,V>
{
    public SynchronizedHashMap()
    {
        super();
    }

    public SynchronizedHashMap(Map<? extends K, ? extends V> m)
    {
        super(m);
    }

    public SynchronizedHashMap(int initialCapacity)
    {
        super(initialCapacity);
    }

    public SynchronizedHashMap(int initialCapacity, float loadFactor)
    {
        super(initialCapacity, loadFactor);
    }

    
    @Override
    synchronized public V put(K key, V val)
    {
        return super.put(key, val);
    }

    @Override
    synchronized public V get(Object key)
    {
        return super.get(key);
    }

    @Override
    synchronized public boolean isEmpty()
    {
        return super.isEmpty(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public boolean containsKey(Object key)
    {
        return super.containsKey(key); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public int size()
    {
        return super.size(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public void clear()
    {
        super.clear(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public void putAll(Map<? extends K, ? extends V> m)
    {
        super.putAll(m); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public V remove(Object key)
    {
        return super.remove(key); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public Set<K> keySet()
    {
        return super.keySet(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public Collection<V> values()
    {
        return super.values(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public boolean containsValue(Object value)
    {
        return super.containsValue(value); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public Object clone()
    {
        return super.clone(); //To change body of generated methods, choose Tools | Templates.
    }

    
    @Override
    synchronized public boolean equals(Object o)
    {
        return super.equals(o); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public int hashCode()
    {
        return super.hashCode(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    synchronized public String toString()
    {
        return super.toString(); //To change body of generated methods, choose Tools | Templates.
    }
    
    
}
