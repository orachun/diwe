/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.communication;

/**
 *
 * @author udomo
 */
public class FileTransferException extends Exception
{

    public FileTransferException()
    {
    }

    public FileTransferException(String message)
    {
        super(message);
    }

    public FileTransferException(Throwable cause)
    {
        super(cause);
    }

    public FileTransferException(String message, Throwable cause)
    {
        super(message, cause);
    }
    
}
