/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine;

import workflowengine.communication.Communicator;
import workflowengine.communication.message.Message;

/**
 *
 * @author orachun
 */
public class Shutdown
{

    public static void main(String[] args)
    {
        try
        {
            Message msg = new Message(Message.TYPE_SHUTDOWN);
            Communicator comm = new Communicator("Shutdown");
            comm.sendMessage(args[0], Integer.parseInt(args[1]), msg);
        }
        catch (Exception ex)
        {
        }
    }
}
