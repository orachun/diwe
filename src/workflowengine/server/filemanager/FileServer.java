/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.server.filemanager;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import workflowengine.utils.Utils;

/**
 *
 * @author orachun
 */
public class FileServer
{
	private static final HashMap<Integer, FileServer> instants = new HashMap<>();
	
	public static final int TYPE_UPLOAD_REQ = 1;
	public static final int TYPE_DOWNLOAD_REQ = 2;
	private static final int PORT_SHIFT = 10;
	public static final int DEFAULT_PORT = Utils.getIntProp("local_port")+PORT_SHIFT;
	private int port;
	private String workingDir;
	private FileServer(String workingDir, int port) throws IOException
	{
		this.port = port;
		this.workingDir = workingDir;
		startListeningThread();
	}

	public static FileServer get()
	{
		return instants.get(DEFAULT_PORT);
	}
	
	public static FileServer get(String workingDir) throws IOException
	{
		return get(workingDir, DEFAULT_PORT);
	}
	
	private static FileServer get(String workingDir, int port) throws IOException
	{
		FileServer fs = instants.get(port);
		if(fs == null)
		{
			fs = new FileServer(workingDir, port);
			instants.put(port, fs);
		}
		return fs;
	}
	
	
	
	private void startListeningThread() throws IOException
	{
		final ServerSocket ss = new ServerSocket(port);
		new Thread("File listening thread")
		{
			@Override
			public void run()
			{
				System.out.println("File server started on port "+port);
				while (true)
				{
					try
					{
						final Socket s = ss.accept();
						new Thread()
						{
							@Override
							public void run()
							{
								try
								{
									requestAccepted(s);
								}
								catch (IOException ex)
								{
									Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, null, ex);
								}
							}
						}.start();
					}
					catch (IOException ex)
					{
						Logger.getLogger(FileServer.class.getName()).log(Level.SEVERE, null, ex);
					}
				}
			}
		}.start();
	}

	public int getPort()
	{
		return port;
	}

	public static int getPort(int port)
	{
		return port + PORT_SHIFT;
	}
	
	
	private void requestAccepted(Socket s) throws IOException
	{
		InputStream in = s.getInputStream();
		OutputStream out = s.getOutputStream();

		int type = in.read();
		int fileNameLen = in.read();
		byte[] fileNameBytes = new byte[fileNameLen];
		in.read(fileNameBytes);
		String filename = workingDir + '/' + new String(fileNameBytes);
		if (type == TYPE_UPLOAD_REQ)
		{
			FileOutputStream fos = new FileOutputStream(filename);
			ReadableByteChannel inch = Channels.newChannel(in);
			FileChannel fch = fos.getChannel();
			long offset = 0;
			long count;
			while ((count = fch.transferFrom(inch, offset, 50000)) > 0)
			{
				offset += count;
			}
			fos.getFD().sync();
			fos.close();
		}
		else
		{
			FileInputStream fis = new FileInputStream(filename);
			WritableByteChannel outch = Channels.newChannel(out);
			FileChannel fch = fis.getChannel();
			long offset = 0;
			long count;
			while ((count = fch.transferTo(offset, 50000, outch)) > 0)
			{
				offset += count;
			}
			fis.close();
		}
		s.close();
	}

	public static long request(String workingDir, String filename, int type, String uri) throws IOException
	{
		String[] host = uri.split(":");
		return request(workingDir, filename, type, host[0], Integer.parseInt(host[1])+PORT_SHIFT);
	}
	
	public static long request(String workingDir, String filename, int type, String host, int port) throws IOException
	{
		Socket s = new Socket(host, port);
		InputStream in = s.getInputStream();
		OutputStream out = s.getOutputStream();

		byte[] filenameBytes = filename.getBytes();

		out.write(type);
		out.write(filenameBytes.length);
		out.write(filenameBytes);

		filename = workingDir + '/' + filename;
		
		long bytesSent = 0;
		if (type == TYPE_DOWNLOAD_REQ)
		{
			FileOutputStream fos = new FileOutputStream(filename);
			ReadableByteChannel inch = Channels.newChannel(in);
			FileChannel fch = fos.getChannel();
			long count;
			while ((count = fch.transferFrom(inch, bytesSent, 50000)) > 0)
			{
				bytesSent += count;
			}
			fos.getFD().sync();
			fos.close();
		}
		else
		{
			FileInputStream fis = new FileInputStream(filename);
			WritableByteChannel outch = Channels.newChannel(out);
			FileChannel fch = fis.getChannel();
			long count;
			while ((count = fch.transferTo(bytesSent, 50000, outch)) > 0)
			{
				bytesSent += count;
			}
			fis.close();
		}
		s.close();
		return bytesSent;
	}
}
