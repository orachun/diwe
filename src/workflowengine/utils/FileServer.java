///*
// * To change this template, choose Tools | Templates
// * and open the template in the editor.
// */
//package workflowengine.utils;
//
//import java.io.DataInputStream;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.net.ServerSocket;
//import java.net.Socket;
//
///**
// *
// * @author orachun
// */
//public class FileServer
//{
//
//	private ServerSocket ss;
//
//	public FileServer(int port)
//	{
//		try
//		{
//			ss = new ServerSocket(port);
//		}
//		catch (IOException e)
//		{
//			e.printStackTrace();
//		}
//	}
//
//	public static boolean waitForFile(String fname)
//	{
//		FileServer fs = new FileServer(Utils.getIntProp("local_port"));
//		try
//		{
//			Socket clientSock = fs.ss.accept();
//			fs.saveFile(clientSock, fname);
//			return true;
//		}
//		catch (IOException e)
//		{
//			e.printStackTrace();
//			return false;
//		}
//
//	}
//
//	private void saveFile(Socket clientSock, String fname) throws IOException
//	{
//		DataInputStream dis = new DataInputStream(clientSock.getInputStream());
//		FileOutputStream fos = new FileOutputStream(fname);
//		byte[] buffer = new byte[4096];
//		int read;
//		while ((read = dis.read(buffer)) > 0)
//		{
//			fos.write(buffer, 0, read);
//		}
//
//		fos.close();
//		dis.close();
//	}
//
//	public static void main(String[] args)
//	{
//		FileServer fs = new FileServer(1988);
//	}
//}
