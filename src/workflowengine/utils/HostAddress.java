/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.Serializable;
import java.util.Properties;

/**
 *
 * @author udomo
 */
public class HostAddress implements Serializable
{
    private String host;
    private int port;

    public HostAddress(Properties p, String hostkey, String portkey)
    {
        host = p.getProperty(hostkey); 
        port = Integer.parseInt(p.getProperty(portkey));
    }
    
    public HostAddress(String host, int port)
    {
        this.host = host;
        this.port = port;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }
    
	/**
	 * 
	 * @return the address in the form of host:port
	 */
    @Override
    public String toString()
    {
        return host+":"+port;
    }
    
}
