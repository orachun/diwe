/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;
 import static workflowengine.server.filemanager.difm.NewDIFM.PIECE_LEN;
/**
 *
 * @author orachun
 */
public class DIFMFile
{
	final private String name;
	private FileChannel channel;
	final private AtomicBitSet existing;
	final private Piece[] pieces; //piece meta-data
	private boolean requiring;
	private boolean active;
	final private double priority;
	private int requiringPeers;
	final private SortedSet<Piece> rarestPcs;
	private AtomicBitSet requesting;
	private long length;

	public DIFMFile(String name, long length, double priority, int requiringPeers, boolean required)
	{
		this.name = name;
		this.active = true;
		this.priority = priority;
		this.requiringPeers = requiringPeers;
		this.requiring = required;
		this.length = length;
		int pieceCount = (int)Math.ceil(length/(double)PIECE_LEN);
		pieces = new Piece[pieceCount];
		rarestPcs = Collections.synchronizedSortedSet(
				new TreeSet<>(DIUtils.getPieceComparator()));
		for(int i=0;i<pieceCount;i++)
		{
			Piece p = new Piece(name, i);
			pieces[i] = p;
			rarestPcs.add(p);
		}
		
		existing = new AtomicBitSet(pieceCount);
		requesting = new AtomicBitSet(pieceCount);
		try
		{
			File f = new File(Utils.getProp("working_dir") + "/" + name);
			Utils.mkdirs(f.getParent());
			f.createNewFile();
			channel = new RandomAccessFile(f, "rw").getChannel();
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFMFile.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	/**
	 * Set whether this file is required by the local peer
	 * @param r 
	 */
	public void setRequired(boolean r)
	{
		requiring = r;
	}

	public boolean isRequired()
	{
		return requiring;
	}
	
	
	
	/**
	 * Set this file as completely downloaded
	 */
	public void setComplete()
	{
		existing.set(0, pieces.length);
	}
	
	/**
	 * Check if the file is completely downloaded
	 * @return 
	 */
	public boolean isCompleted()
	{
		return pieces.length == existing.cardinality();
	}
	
	public void setRequesting(int index, boolean r)
	{
		if(r)
		{
			requesting.set(index);
		}
		else
		{
			requesting.clear(index);
		}
	}

	public AtomicBitSet getRequesting()
	{
		return requesting;
	}
	
	
		
	public AtomicBitSet getExisting()
	{
		return existing;
	}

	/**
	 * Number of inexistent pieces
	 * @return 
	 */
	public int getRemainingPcs()
	{
		return pieces.length - existing.cardinality();
	}
	/**
	 * Number of existing pieces
	 * @return 
	 */
	public int getDownloadedPcs()
	{
		return existing.cardinality();
	}
	
	public double getPercentDownloaded()
	{
		return existing.cardinality()/(double)pieces.length*100;
	}
	
	public int getRequiringPeers()
	{
		return requiringPeers;
	}
	
	public void reduceRequiringPeers()
	{
		requiringPeers--;
	}
	
	public void setInactive()
	{
		this.active = false;
	}

	public boolean isActive()
	{
		return active;
	}
	
	public int getPieceLength(int index)
	{
		return (int)((index == pieces.length-1) ? length % PIECE_LEN : PIECE_LEN);
	}
	
	/**
	 * Record retrieved piece into the file system
	 * @param p 
	 */
	public void write(int pieceIndex, byte[] content, int length)
	{
//		if(isCompleted())
//		{
//			return;
//		}
		ByteBuffer contentBuffer = ByteBuffer.wrap(content);
		try
		{
			final Piece p = pieces[pieceIndex];
			synchronized(p)
			{
				if(!existing.get(pieceIndex))
				{
					long offset = p.index*PIECE_LEN;
					channel.position(offset);
					if(channel.write(contentBuffer, offset) != length)
					{
						throw new IOException("Cannot write all bytes");
					}
					existing.set(pieceIndex);
				}
			}
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFMFile.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public byte[] read(int pieceIndex, int length)
	{
		ByteBuffer content = ByteBuffer.allocate(length);
		try
		{
			Piece p = pieces[pieceIndex];
			long offset = p.index*PIECE_LEN;
			channel.position(offset);
			if(channel.read(content, offset) != length)
			{
				throw new IOException("Cannot read all bytes");
			}
			
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFMFile.class.getName()).log(Level.SEVERE, null, ex);
		}
		return content.array();
	}
	
	public void finallized()
	{
		try
		{
			//write content to storage device and change to read-only mode
			channel.force(true);
			channel.close();
			
			channel = new RandomAccessFile(Utils.getProp("working_dir") + "/" + name, "r")
					.getChannel();
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFMFile.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public Piece getPiece(int index)
	{
		return pieces[index];
	}

	public String getName()
	{
		return name;
	}

	public double getPriority()
	{
		return priority;
	}
	
	

	@Override
	public boolean equals(Object obj)
	{
		if(this == obj)
		{
			return true;
		}
		if(obj instanceof DIFMFile)
		{
			return this.name.equals(((DIFMFile)obj).name);
		}
		return false;
	}

	
	public void incrementPieceSeen(AtomicBitSet available)
	{
		for (int i = available.nextSetBit(0); i >= 0; i = available.nextSetBit(i + 1))
		{
			seenPiece(i);
		}
		
	}
	
	
	private ReentrantReadWriteLock rarestLocks = new ReentrantReadWriteLock(true);
	public void seenPiece(int index)
	{
		rarestLocks.readLock().lock();
		rarestPcs.remove(pieces[index]);
		pieces[index].seen();
		rarestPcs.add(pieces[index]);
		rarestLocks.readLock().unlock();
	}
	
	public Piece getPieceToDownload(AtomicBitSet interesting)
	{
		if(interesting.cardinality() == 0)
		{
			return null;
		}
		rarestLocks.writeLock().lock();
		Piece p = DIUtils.getPieceProportionally(rarestPcs, 20, interesting);
		rarestLocks.writeLock().unlock();
		return p;
	}
	
}
