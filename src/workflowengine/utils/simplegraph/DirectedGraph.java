/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils.simplegraph;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;


/**
 *
 * @author orachun
 */
public class DirectedGraph<T> implements Serializable
{
	public static final int PARENT_LIST = 0;
	public static final int CHILD_LIST = 1;
    private HashMap<T, Set<T>[]> nodeMap = new HashMap<>();
	private Set<T> rootNodes = new HashSet<>();
	private Set<T> leafNodes = new HashSet<>();

    public DirectedGraph()
    {
    }
    
	public void addNode(T n)
	{
		if(!nodeMap.containsKey(n))
		{
			nodeMap.put(n, new Set[]{new HashSet<>(),new HashSet<>()});
			rootNodes.add(n);
			leafNodes.add(n);
		}
	}
	public int size()
	{
		return nodeMap.size();
	}
	public void addNodes(T from, T to)
	{
		addNode(from);
		addNode(to);
		nodeMap.get(from)[CHILD_LIST].add(to);
		nodeMap.get(to)[PARENT_LIST].add(from);
		rootNodes.remove(to);
		leafNodes.remove(from);
	}
    
	public Set<T> getParent(T n)
	{
		Set<T>[] sets = nodeMap.get(n);
		if(sets == null)
		{
			return null;
		}
		return Collections.unmodifiableSet(sets[PARENT_LIST]);
	}
	public Set<T> getChild(T n)
	{
		Set<T>[] sets = nodeMap.get(n);
		if(sets == null)
		{
			return null;
		}
		return Collections.unmodifiableSet(sets[CHILD_LIST]);
	}
	public Set<T> getNodeSet()
	{
		return Collections.unmodifiableSet(nodeMap.keySet());
	}
	public Set<T> getRoots()
	{
		return Collections.unmodifiableSet(rootNodes);
	}
	public Set<T> getLeaves()
	{
		return Collections.unmodifiableSet(leafNodes);
	}
	
	/**
	 * Get a list of ordered tasks by their dependencies
	 * @return a list of tasks
	 */
	public LinkedList<T> getOrderedNodes()
	{
		LinkedList<T> queue = new LinkedList<>();
		T n;
		Set<T> parents;
        LinkedList<T> q = new LinkedList<>(leafNodes);
        while(!q.isEmpty())
        {
            n = q.poll();
            queue.remove(n);
            queue.push(n);
            parents = this.getParent(n);
            q.removeAll(parents);
            q.addAll(parents);
        }
        return queue;
	}
	
	public Set<Map.Entry<T,Set<T>[]>> entrySet()
	{
		return nodeMap.entrySet();
	}
}
