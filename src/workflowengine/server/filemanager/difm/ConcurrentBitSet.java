/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 *
 * @author orachun
 */
public class ConcurrentBitSet
{

	private final AtomicIntegerArray array;
	private final int length;

	public ConcurrentBitSet(int length)
	{
		this.length = length;
		int intLength = (length + 31) / 32;
		array = new AtomicIntegerArray(intLength);
	}

	public void set(int n)
	{
		int bit = 1 << n;
		int idx = (int) (n >>> 5);
		while (true)
		{
			int num = array.get(idx);
			int num2 = num | bit;
			if (num == num2 || array.compareAndSet(idx, num, num2))
			{
				return;
			}
		}
	}

	public boolean get(int n)
	{
		int bit = 1 << n;
		int idx = (int) (n >>> 5);
		int num = array.get(idx);
		return (num & bit) != 0;
	}
	
	
	public int size()
	{
		return length;
	}
}
