/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 *
 * @author orachun
 */
public interface PeerInterface
{
	public enum MsgType
	{
		PEER_CONNECTED,		//msg -> (String)uri
		PEER_LIST,			//msg -> (String[])uris
		FILE_REQ_INFO,		//msg -> (Map[2])wantingFiles, wantingPeers
		FILE_INFO,			//msg -> (Map[])file info (Keys: name, length, priority)
		FILE_INACTIVATE		//msg -> (String)name
	}
	public BitSet getExistingPcs(String file);
	public ByteBuffer getPieceContent(String name, int index);
	public Object processMsg(MsgType t, Object msg, String from);
}
