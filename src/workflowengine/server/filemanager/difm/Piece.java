/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.util.Objects;

/**
 *
 * @author orachun
 */
public class Piece
{
	final String name;
	final int index;
	final long offset;
	private int seen;
	final long length;

	public Piece(String name, int index, long offset, long length)
	{
		this.name = name;
		this.index = index;
		this.offset = offset;
		this.seen = 0;
		this.length = length;
	}
	
	public void seen()
	{
		seen++;
	}
	
	public int getSeen()
	{
		return seen;
	}

	@Override
	public boolean equals(Object obj)
	{
		if(obj instanceof Piece)
		{
			Piece p = (Piece)obj;
			return name.equals(p.name) && index==p.index;
		}
		return false;
	}

	@Override
	public int hashCode()
	{
		int hash = 7;
		hash = 23 * hash + Objects.hashCode(this.name);
		hash = 23 * hash + this.index;
		return hash;
	}
	
	
	
}
