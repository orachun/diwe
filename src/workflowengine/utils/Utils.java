/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package workflowengine.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import removed.TaskManager;
import workflowengine.workflow.Workflow;
//import org.apache.commons.vfs.FileSystemException;

/**
 *
 * @author Orachun
 */
public class Utils
{

    public static final double BYTE = 1.0 / 1024.0 / 1024.0;
    public static final double KB = 1.0 / 1024.0;
    public static final double MB = 1;
    public static final double GB = 1024;
    public static final double TB = 1024 * 1024;
    private static final Properties PROP = new Properties();
    private static final String CONFIG_FILE = "default.properties";
    private static boolean isPropInited = false;
    private static final int BUFFER_LEN = 1024*1024; //length of buffer in bytes


    public static Properties getPROP()
    {
        initProp();
        return PROP;
    }
    
    public static void disableDB()
    {
        initProp();
        PROP.setProperty("db_disabled", "true");
    }
    public static void enableDB()
    {
        initProp();
        PROP.setProperty("db_disabled", "false");
    }
    public static boolean isDBEnabled()
    {
        initProp();
        String disabled = PROP.getProperty("db_disabled");
        return !disabled.equals("true");
    }
    
    public static void initProp()
    {
        if (!isPropInited)
        {
            try
            {
                InputStreamReader is = new FileReader(CONFIG_FILE);
                PROP.load(is);
                is.close();
                PROP.setProperty("home_dir", System.getProperty("user.home"));
                if(!PROP.containsKey("db_disabled"))
                {
                    PROP.setProperty("db_disabled", "false");
                }
                isPropInited = true;
            }
            catch (IOException ex)
            {
                System.err.println("Cannot read the configuration file " + CONFIG_FILE + ".");
                throw new RuntimeException("Cannot read the configuration file " + CONFIG_FILE + ".");
            }
        }
    }
    
    public static void setProp(Properties p)
    {
        initProp();
        PROP.putAll(p);
    }
    
    public static void setPropIfNotExist(String name, String val)
    {
        initProp();
        if(!PROP.containsKey(name))
        {
            PROP.setProperty(name, val);
        }
    }

    public static boolean hasProp(String name)
    {
        initProp();
        return PROP.containsKey(name);
    }
    
    public static String getProp(String name)
    {
        initProp();
        return PROP.getProperty(name);
    }

    public static int getIntProp(String name)
    {
        String s = getProp(name);
        return s == null ? null : Integer.parseInt(s);
    }

    public static double getDoubleProp(String name)
    {
        String s = getProp(name);
        return s == null ? null : Double.parseDouble(s);
    }

    public static long time()
    {
        return (long) Math.round(System.currentTimeMillis() / 1000.0);
    }

    public static String uuid()
    {
        return UUID.randomUUID().toString();
    }
    
    /**
     * Return a UUID string which ensure that it is not in the given
     * collection.
     * @param excludes
     * @return 
     */
    public static String uuid(Collection<String> excludes)
    {
        String uuid = uuid();
        while(excludes.contains(uuid))
        {
            uuid = uuid();
        }
        return uuid;
    }

    public static String execAndWait(String[] cmds, boolean getOutput)
    {
        try
        {
            Process p = Runtime.getRuntime().exec(cmds);
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder sb = null;
            if (getOutput)
            {
                sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null)
                {
                    sb.append(line).append('\n');
                }
            }
            p.waitFor();
            return getOutput ? sb.toString() : "";
        }
        catch (IOException | InterruptedException ex)
        {
            return null;
        }
    }

    public static boolean exec(String[] cmds)
    {
        try
        {
            Runtime.getRuntime().exec(cmds);
            return true;
        }
        catch (IOException ex)
        {
            return false;
        }
    }

    public static boolean isFileExist(String path)
    {
        return new File(path).exists();
    }

    public static boolean isDir(String path)
    {
        return new File(path).isDirectory();
    }

    public static void setExecutable(String path)
    {
        new File(path).setExecutable(true);
    }

    public static void setExecutableInDirSince(String dirPath, final long since)
    {
        File[] files = new File(dirPath).listFiles(new FileFilter()
        {
            @Override
            public boolean accept(File pathname)
            {
                if(since == -1)
                {
                    return true;
                }
                return pathname.lastModified() > since;
            }
        });
        for (File f : files)
        {
            f.setExecutable(true);
        }
    }

    public static void createDir(String path)
    {
        new File(path).mkdirs();
    }

    public static void printMap(OutputStream out, Map m)
    {
        PrintWriter pw = new PrintWriter(out);
        pw.println("Printing map ...");
        for (Object k : m.keySet())
        {
            pw.println(k + " ===== " + m.get(k));
            pw.println();
        }
        pw.println("----------------");
        pw.flush();
    }

    public static void pipe(InputStream in, OutputStream out) throws IOException
    {
        byte[] buffer = new byte[BUFFER_LEN];
        int len = 1;
        while (len > -1)
        {
            len = in.read(buffer);
            if(len > -1)
            {
                out.write(buffer, 0, len);
            }
        }
        out.flush();
    }
    
//    public static void streamToFile(InputStream in, String filepath, long offset, long length) throws IOException
//    {
//        FileOutputStream fos = new FileOutputStream(filepath);
//        FileChannel fc = fos.getChannel();
//        fc.position(offset);
//        ByteBuffer bb = ByteBuffer.allocate(BUFFER_LEN);
//        byte[] buffer = new byte[BUFFER_LEN];
//        long count = 0;
//        int readSize;
//        while(count < length)
//        {
//            readSize = in.read(buffer);
//            bb.put(buffer, 0, readSize);
//            fc.write(bb);
//        }
//        fc.close();
//    }
    
    public static String[] getfileListInDir(String dirPath)
    {
        if(!Utils.isFileExist(dirPath))
        {
            return new String[0];
        }
        File file = new File(dirPath).getAbsoluteFile();
        LinkedList<String> fileList = new LinkedList<>();
        LinkedList<File> q = new LinkedList<>();
        q.push(file);
        while(!q.isEmpty())
        {
            File f = q.pop();
            if(f.isDirectory())
            {
                for(File childFile : f.listFiles())
                {
                    q.push(childFile);
                }
            }
            else
            {
                fileList.add(f.getAbsolutePath());
            }
        }
        return fileList.toArray(new String[]{});
    }
    
    public static String getParentPath(String filePath)
    {
        return new File(filePath).getParent();
    }
    
    
    public static String[] getAllfilesInDir(String dirPath)
    {
        File file = new File(dirPath);
        LinkedList<String> fileList = new LinkedList<>();
        LinkedList<File> q = new LinkedList<>();
        q.push(file);
        while(!q.isEmpty())
        {
            File f = q.pop();
            if(f.isDirectory())
            {
                for(File childFile : f.listFiles())
                {
                    q.push(childFile);
                }
            }
            else
            {
                fileList.add(f.getAbsolutePath());
            }
        }
        return fileList.toArray(new String[]{});
    }
    
    public static ProcessBuilder createProcessBuilder(String[] cmds, 
            String workingDir, String stdoutFile, String stderrFile, 
            String additionalPath)
    {
        ProcessBuilder pb = new ProcessBuilder(cmds).directory(new File(
                workingDir));
        if(stderrFile != null && !stderrFile.isEmpty())
        {
            pb.redirectError(new File(stderrFile));
        }
        if(stdoutFile != null && !stdoutFile.isEmpty())
        {
            pb.redirectOutput(new File(stdoutFile));
        }
        String path = pb.environment().get("PATH") 
                + ":" + Utils.getProp("additional_path");
        if(additionalPath != null && !additionalPath.isEmpty())
        {
            path += ":" + additionalPath;
        }
        path += ":" + workingDir;
        pb.environment().put("PATH", path);
        
        return pb;
    }
    public static ProcessBuilder createProcessBuilder(String[] cmds)
    {
        return createProcessBuilder(cmds, Utils.getProp("home_dir"), null, null, null);
    }
    
    /**
     * Rename file. wildcard is enabled in file path
     * @param filePath
     * @param newFilePath 
     */
    public static void renameFile(String filePath, String newFilePath)
    {
        File f = new File(filePath);
        FileFilter ff = new WildcardFileFilter(f.getName());
        File[] files = f.getParentFile().listFiles(ff);
        if(files.length == 1)
        {
            files[0].renameTo(new File(newFilePath));
        }
    }
    
    /**
     * Get file list from wildcard
     * @param filePath
     * @return 
     */
    public static File[] fileFromWildcard(String filePath)
    {
        File f = new File(filePath);
        FileFilter ff = new WildcardFileFilter(f.getName());
        return f.getParentFile().listFiles(ff);
    }
   
    
    
    public static boolean writeToFile(Object o, String filename)
    {
        try
        {
            FileOutputStream fout = new FileOutputStream(filename);
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(o);
            oos.close();
            return true;
        }
        catch(IOException ex)
        {
            return false;
        }
    }
    public static Object readFromFile(String filename)
    {
        try
        {
            FileInputStream fout = new FileInputStream(filename);
            ObjectInputStream oos = new ObjectInputStream(fout);
            Object o = oos.readObject();
            oos.close();
            return o;
        }
        catch(IOException | ClassNotFoundException ex)
        {
            return null;
        }
    }
    
    public static Logger getLogger()
    {
        return new Logger(Utils.getProp("log_file"));
    }
    
    public static boolean isProcTerminated(Process p)
    {
        try
        {
            p.exitValue();
            return true;
        }
        catch(IllegalThreadStateException e)
        {
            return false;
        }
    }
    
    public static boolean waitFor(Process p, final long timeoutSeconds)
    {
        final Thread current = Thread.currentThread();
        Thread waitThread = new Thread(new Runnable() {
            @Override
            public void run()
            {
                try
                {
                    Thread.sleep(500+timeoutSeconds*1000);
                    current.interrupt();
                }
                catch (InterruptedException ex)
                {}
            }
        });
        waitThread.start();
        try
        {
            p.waitFor();
            waitThread.interrupt();
            return true;
        }
        catch(InterruptedException e)
        {
            return false;
        }
    }
    
    public static String getLastModifiedFileInDir(String dir)
    {
        File d = new File(dir);
        File[] files = d.listFiles();
        File last = null;
        for(File f : files)
        {
            if(last == null || last.lastModified() < f.lastModified())
            {
                last = f;
            }
        }
        return last.getAbsolutePath();
    }
    
	public static void writeObjectToFile(Serializable obj, String filename) throws IOException
	{
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename));
		oos.writeObject(obj);
		oos.close();
	}
	public static Object readObjectFromFile(String filename) throws ClassNotFoundException, IOException
	{
		FileInputStream fin = new FileInputStream(filename);
		ObjectInputStream ois = new ObjectInputStream(fin);
		Object o = ois.readObject();
		ois.close();
		return o;
	}
}
