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
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class DIFMFile
{
	private static int PIECE_LEN = 50*1024*1024; //50MB
	private String name;
	private FileChannel channel;
	private AtomicBitSet existing;
	private Piece[] pieces; //piece meta-data
	private boolean requiring;
	private boolean active;
	private double priority;
	private int requiringPeers;
	private SortedSet<Piece> rarestPcs;

	public DIFMFile(String name, long length, double priority, int requiringPeers)
	{
		this.name = name;
		this.active = true;
		this.priority = priority;
		this.requiringPeers = requiringPeers;
		int pieceCount = (int)Math.ceil(length/(double)PIECE_LEN);
		pieces = new Piece[pieceCount];
		rarestPcs = Collections.synchronizedSortedSet(
				new TreeSet<>(DIUtils.getPieceComparator()));
		for(int i=0;i<pieceCount;i++)
		{
			int pcsLen = (int)((i == pieceCount-1) ? length % PIECE_LEN : PIECE_LEN);
			Piece p = new Piece(name, i, i*PIECE_LEN, pcsLen);
			pieces[i] = p;
			rarestPcs.add(p);
		}
		
		existing = new AtomicBitSet(pieceCount);
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
	public void setIfRequired(boolean r)
	{
		requiring = r;
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
		return existing.cardinality() == pieces.length;
	}
		
	public AtomicBitSet getExisting()
	{
		return existing;
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
	
	
	/**
	 * Record retrieved piece into the file system
	 * @param p 
	 */
	public void write(int pieceIndex, byte[] content, int length)
	{
		if(isCompleted())
		{
			return;
		}
		ByteBuffer contentBuffer = ByteBuffer.wrap(content);
		try
		{
			final Piece p = pieces[pieceIndex];
			synchronized(p)
			{
				if(!existing.get(pieceIndex))
				{
					channel.position(p.offset);
					if(channel.write(contentBuffer, p.offset) != length)
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
			channel.position(p.offset);
			if(channel.read(content, p.offset) != length)
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
		for(int i=available.nextSetBit(0);i>=0;i=available.nextSetBit(i+1))
		{
			rarestPcs.remove(pieces[i]);
			pieces[i].seen();
			rarestPcs.add(pieces[i]);
		}
	}
	
	public Piece getPieceToDownload(AtomicBitSet interesting)
	{
		return DIUtils.getPieceProportionally(rarestPcs, 5, interesting);
	}

	
}
