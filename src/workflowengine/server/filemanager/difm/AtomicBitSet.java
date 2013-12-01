/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.io.Serializable;
import java.util.BitSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * @author orachun
 */
public class AtomicBitSet implements Serializable, Cloneable
{
	private BitSet bitSet;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final Lock read = readWriteLock.readLock();
	private final Lock write = readWriteLock.writeLock();

	public AtomicBitSet(int nBits)
	{
		write.lock();
		this.bitSet = new BitSet(nBits);
		write.unlock();
	}
	
	private AtomicBitSet(BitSet set)
	{
		write.lock();
		this.bitSet = (BitSet)set.clone();
		write.unlock();
	}
		
	
	
	public  void set(int bitIndex)
	{
		write.lock();
		bitSet.set(bitIndex);
		write.unlock();
	}

	public  void set(int fromIndex, int toIndex)
	{
		write.lock();
		bitSet.set(fromIndex, toIndex);
		write.unlock();
	}
	
	
	
	public  void clear(int bitIndex)
	{
		write.lock();
		bitSet.clear(bitIndex);
		write.unlock();
	}
		
	public  boolean get(int bitIndex)
	{
		read.lock();
		boolean r = bitSet.get(bitIndex);
		read.unlock();
		return r;
	}

	
	public  int nextSetBit(int fromIndex)
	{
		read.lock();
		int r = bitSet.nextSetBit(fromIndex);
		read.unlock();
		return r;
	}

	
	public  int nextClearBit(int fromIndex)
	{
		read.lock();
		int r = bitSet.nextClearBit(fromIndex);
		read.unlock();
		return r;
	}

	
	public  int previousSetBit(int fromIndex)
	{
		read.lock();
		int r = bitSet.previousSetBit(fromIndex);
		read.unlock();
		return r;
	}

	
	public  int previousClearBit(int fromIndex)
	{
		read.lock();
		int r = bitSet.previousClearBit(fromIndex);
		read.unlock();
		return r;
	}
	
	public  int cardinality()
	{
		read.lock();
		int r = bitSet.cardinality();
		read.unlock();
		return r;
	}

	
	public  void and(AtomicBitSet set)
	{
		write.lock();
		bitSet.and(set.bitSet);
		write.unlock();
	}

	
	public  void or(AtomicBitSet set)
	{
		write.lock();
		bitSet.or(set.bitSet);
		write.unlock();
	}

	
	public  void xor(AtomicBitSet set)
	{
		write.lock();
		bitSet.xor(set.bitSet);
		write.unlock();
	}

	
	public  void andNot(AtomicBitSet set)
	{
		write.lock();
		bitSet.andNot(set.bitSet);
		write.unlock();
	}

		
	public  int size()
	{
		read.lock();
		int r = bitSet.size();
		read.unlock();
		return r;
	}
	
	public  Object clone()
	{
		read.lock();
		AtomicBitSet r = new AtomicBitSet(this.bitSet);
		read.unlock();
		return r;
	}
	
}
