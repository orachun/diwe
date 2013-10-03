/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import workflowengine.server.WorkflowExecutor;

/**
 *
 * @author orachun
 */
public class Shutdown
{

    public static void main(String[] args) throws NotBoundException, RemoteException
    {
        WorkflowExecutor.getRemoteExecutor(args[0]).shutdown();
    }
}
