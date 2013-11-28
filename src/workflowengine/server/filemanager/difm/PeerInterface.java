/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager.difm;

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
	public AtomicBitSet getExistingPcs(String file);
	public byte[] getPieceContent(String name, int index);
	public Object processMsg(MsgType t, Object msg, String from);
	@Override
	public String toString();
}
