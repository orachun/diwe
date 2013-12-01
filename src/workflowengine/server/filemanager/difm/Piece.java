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
	private int seen;

	public Piece(String name, int index)
	{
		this.name = name;
		this.index = index;
		this.seen = 0;
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
		if(this == obj)
		{
			return true;
		}
		if(obj instanceof Piece)
		{
			Piece p = (Piece)obj;
			return index == p.index && name.equals(p.name);
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

	@Override
	public String toString()
	{
		return name+"("+index+")";
	}
	
	
	
}
