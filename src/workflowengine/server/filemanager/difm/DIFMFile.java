/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author orachun
 */
public class DIFMFile
{
	private static int PIECE_LEN;
	private String name;
	private FileChannel channel;
	private BitSet existing;
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
		rarestPcs = new TreeSet<>(DIUtils.getPieceComparator());
		for(int i=0;i<pieceCount;i++)
		{
			long pcsLen = (i == pieceCount-1) ? length % PIECE_LEN : PIECE_LEN;
			Piece p = new Piece(name, i, i*PIECE_LEN, pcsLen);
			pieces[i] = p;
			rarestPcs.add(p);
		}
		
		existing = new BitSet(pieceCount);
		try
		{
			channel = new RandomAccessFile(name, "rw").getChannel();
		}
		catch (FileNotFoundException ex)
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
	
	/**
	 * Set that the piece is received
	 * @param index 
	 */
	public void setReceived(int index)
	{
		existing.set(index);
	}
	
	public BitSet getExisting()
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
	public void write(int pieceIndex, ByteBuffer content, double length)
	{
		try
		{
			Piece p = pieces[pieceIndex];
			channel.position(p.offset);
			if(channel.write(content, p.offset) != length)
			{
				throw new IOException("Cannot write all bytes");
			}
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFMFile.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public ByteBuffer read(int pieceIndex)
	{
		ByteBuffer content = ByteBuffer.allocate(PIECE_LEN);
		try
		{
			Piece p = pieces[pieceIndex];
			channel.position(p.offset);
			if(channel.read(content, p.offset) != PIECE_LEN)
			{
				throw new IOException("Cannot read all bytes");
			}
		}
		catch (IOException ex)
		{
			Logger.getLogger(DIFMFile.class.getName()).log(Level.SEVERE, null, ex);
		}
		return content;
	}
	
	public void finallized()
	{
		try
		{
			channel.close();
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
		if(obj instanceof DIFMFile)
		{
			return this.name.equals(((DIFMFile)obj).name);
		}
		return false;
	}

	
	public void incrementPieceSeen(BitSet available)
	{
		for(int i=available.nextSetBit(0);i<pieces.length;i=available.nextSetBit(i))
		{
			rarestPcs.remove(pieces[i]);
			pieces[i].seen();
			rarestPcs.add(pieces[i]);
		}
	}
	
	public Piece getPieceToDownload()
	{
		return DIUtils.getElementProportionally(rarestPcs, 5);
	}

	
}
