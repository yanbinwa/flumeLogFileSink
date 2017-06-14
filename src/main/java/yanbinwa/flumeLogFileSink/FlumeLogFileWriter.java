package yanbinwa.flumeLogFileSink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 在这里考虑roll back的操作，按照interval比较困难，还是只支持文件大小就好了
 * 
 * 对于同步的要求，write方法需要加锁，不能
 * 
 * @author yanbinwa
 *
 */

public class FlumeLogFileWriter implements Comparable<FlumeLogFileWriter>
{
    public static final String LOGFILE_SUFFIX = ".txt"; 
    private static final Logger logger = LoggerFactory.getLogger(FlumeLogFileWriter.class);
    
    private long changeTimeStamp = 0L;
    
    /* 文件句柄 */
    private FileOutputStream out = null;
    private File logFile = null;
    
    private String rootPath = null;
    private String serviceGroupName = null;
    private String serviceName = null;
    private String logFileName = null;
    private int rollFileSize;
    
    public FlumeLogFileWriter(String rootPath, String serviceGroupName, String serviceName, String logFileName, int rollFileSize)
    {
        this.rootPath = rootPath;
        this.serviceGroupName = serviceGroupName;
        this.serviceName =serviceName;
        this.logFileName = logFileName;
        this.rollFileSize = rollFileSize;
    }
    /**
     * 这里要创建或者打开文件文件夹，同时更新expiryTimeStamp
     * 
     */
    public void start()
    {
        String logFileDirStr = getLogFileDirStr();
        File logFileDir = new File(logFileDirStr);
        if (!logFileDir.exists())
        {
            logger.info("Create the dir: " + logFileDirStr);
            logFileDir.mkdirs();
        }
        else if (!logFileDir.isDirectory())
        {
            logger.error(logFileDirStr + " is not a dir");
            return;
        }
        
        String logFileStr = getLogFilePathStr();
        logFile = new File(logFileStr);
        if (!logFile.exists())
        {
            try
            {
                logFile.createNewFile();
            } 
            catch (IOException e)
            {
                logger.error("Can not create file " + logFileStr);
                e.printStackTrace();
            }
        }
        if (!logFile.isFile())
        {
            logger.error(logFileStr + " is not a file");
            return;
        }
        
        try
        {
            out = new FileOutputStream(logFile);
        } 
        catch (FileNotFoundException e)
        {
            logger.error("Create FileOutputStream failed");
            e.printStackTrace();
        }
        changeTimeStamp = System.currentTimeMillis();
    }
    
    /**
     * 这里要关闭file
     * 
     */
    public void stop()
    {
        if (out != null)
        {
            try
            {
                out.close();
            } 
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        out = null;
        logFile = null;
    }
    
    /* 这里会在每次写入时update expiryTimeStamp，同时会判断文件的大小，如果文件超出，则将之前的文件改名字，并加上时间戳，并重新打开一个文件 */
    public synchronized boolean write(byte[] logMsg)
    {
        if (logFile == null || out == null)
        {
            logger.error("Can not write log to file: " + new String(logMsg) + "; the file can not open");
            return false;
        }
        try
        {
            long fileSize = logFile.length();
            if (fileSize > rollFileSize)
            {
                //关闭原来的stream
                out.close();
                out = null;
                String newFileName = getLogFileDirStr() + "/" + logFileName + "_" + System.currentTimeMillis() + LOGFILE_SUFFIX;
                logFile.renameTo(new File(newFileName));
                String logFileStr = getLogFilePathStr();
                logFile = new File(logFileStr);
                out = new FileOutputStream(logFile);
            } 
            out.write(logMsg);
            out.write("\r\n".getBytes());
            changeTimeStamp = System.currentTimeMillis();
            return true;
        }
        catch (IOException e)
        {
            logger.error("Fault to write the log " + new String(logMsg));
            e.printStackTrace();
            return false;
        }
    }
    
    public String getServiceGroupName()
    {
        return this.serviceGroupName;
    }
    
    public String getServiceName()
    {
        return this.serviceName;
    }
    
    public String getLogFileName()
    {
        return this.logFileName;
    }
    
    public long getChangeTimeStamp()
    {
        return this.changeTimeStamp;
    }
    
    /**
     * 这里只要比较expiryTimeout,expiryTimeout越小
     */
    @Override
    public int compareTo(FlumeLogFileWriter obj)
    {
        if (obj == null)
        {
            return 1;
        }
        if (changeTimeStamp > obj.changeTimeStamp)
        {
            return 1;
        }
        else if (changeTimeStamp == obj.changeTimeStamp)
        {
            return 0;
        }
        else
        {
            return -1;
        }
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (!(obj instanceof FlumeLogFileWriter))
        {
            return false;
        }
        FlumeLogFileWriter other = (FlumeLogFileWriter) obj;
        if (this.serviceGroupName.equals(other.serviceGroupName) && this.serviceName.equals(other.serviceName)
                && this.logFileName.equals(other.logFileName))
        {
            return true;
        }
        else
        {
            return false;
        }
    }
    
    @Override
    public String toString()
    {
        String msg = "serviceGroupName is: " + serviceGroupName + "; "
                + "serviceName is: " + serviceName + "; "
                + "logFileName is: " + logFileName;
        
        return msg;
    }
    
    @Override
    public int hashCode()
    {
        return this.toString().hashCode();
    }
    
    private String getLogFileDirStr()
    {
        return rootPath + "/" + serviceGroupName + "/" + serviceName + "/" + logFileName;
    }
    
    private String getLogFilePathStr()
    {
        return getLogFileDirStr() + "/" + logFileName + LOGFILE_SUFFIX;
    }
}
