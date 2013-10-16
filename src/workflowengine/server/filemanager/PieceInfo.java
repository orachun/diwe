/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

/**
 *
 * @author orachun
 */
public class PieceInfo implements Serializable
{
	public String name;
	public int pieceNo;
	public double priority;
	public long fileLength = -1;
	
	private static HashMap<PieceInfo, PieceInfo> map = new HashMap<>();

	private PieceInfo(String name, int pieceNo, double priority, long fileLength)
	{
		this.name = name;
		this.pieceNo = pieceNo;
		this.priority = priority;
		this.fileLength = fileLength;
	}
	
	public static PieceInfo get(String name, int pieceNo, double priority)
	{
		return get(name, pieceNo, priority, -1);
	}
	
	public static PieceInfo get(String name, int pieceNo, double priority, long fileLength)
	{
		PieceInfo newP = new PieceInfo(name, pieceNo, priority, fileLength);
		PieceInfo p = map.get(newP);
		if(p == null)
		{
			map.put(p,p);
			p = newP;
		}
		else
		{
			p.priority = priority;
		}
		return p;
	}

	@Override
	public int hashCode()
	{
		int hash = 3;
		hash = 89 * hash + Objects.hashCode(this.name);
		hash = 89 * hash + this.pieceNo;
		return hash;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
		{
			return false;
		}
		final PieceInfo other = (PieceInfo) obj;
		if (!Objects.equals(this.name, other.name))
		{
			return false;
		}
		if (this.pieceNo != other.pieceNo)
		{
			return false;
		}
		return true;
	}

	
}