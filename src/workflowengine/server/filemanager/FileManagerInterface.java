/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import com.mongodb.DBObject;
import java.util.List;
import java.util.Set;

/**
 *
 * @author orachun
 */
public interface FileManagerInterface
{
	/**
	 * 
	 * @param list a list of required_pieces exported from db
	 */
//	public void setAllRequiredPieces(Map<String, Set<PieceInfo>> requiredPieces,
//		Map<String, Set<String>> requiringSites);
	
	public void setAllRequiredPieces(String invoker, List<DBObject> list);
	public void setFilePriority(List<DBObject> list);
	public  List<DBObject>  getRetrievedPieces(String invoker);
	
	public void workerJoined(String uri);
	
	public void setPeerSet(Set<String> workers);
}
