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
public interface DIFileManagerInterface
{
	/**
	 * 
	 * @param list a list of required_pieces exported from db
	 */
//	public void setAllRequiredPieces(Map<String, Set<PieceInfo>> requiredPieces,
//		Map<String, Set<String>> requiringSites);
	
	public void setPiecesInfo(String invoker, List<DBObject> requiredPieces, List<DBObject> filePriorities);
//	public void setFilePriority(List<DBObject> list);
	public  List<DBObject>  getRetrievedPieces(String invoker);
	
	public void addWorker(String uri);
	
	public void setPeerSet(Set<String> workers);
}
