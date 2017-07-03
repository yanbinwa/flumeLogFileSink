package yanbinwa.flumeLogFileSink;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * 
 * 这里有一个线程来判断FlumeLogFileWriter的情况，如果超期，就删掉，如果超出了当前Max的数量，就把最老的FlumeLogFile踢掉
 * 
 * 在process中读出event后就写入到queue中，作为缓存
 * 
 * @author yanbinwa
 *
 */

public class FlumeLogFileSink extends AbstractSink implements Configurable
{
    private static final Logger logger = LoggerFactory.getLogger(FlumeLogFileSink.class);
    
    public static final String ROLL_FILE_ROOT_PATH_KEY = "sink.rollFileRootPath";
    /* size or interval */
    public static final String ROLL_FILE_TYPE_KEY = "sink.rollFileType";
    /* the unit is MB */
    public static final String ROLL_FILE_SIZE_KEY = "sink.rollFileSize";
    /* the unit is second */
    public static final String ROLL_FILE_INTERVAL_KEY = "sink.rollFileInterval";
    public static final String ROLL_FILE_MAX_OPEN_FILE_KEY = "sink.maxOpenFile";
    /* the unit is second */
    public static final String ROLL_FILE_EXPIRY_TIMEOUT_KEY = "sink.expiryTimeout";
    
    
    public static final String SERVICEGROUP_NAME_KEY = "serviceGroupName";
    public static final String SERVICE_NAME_KEY = "serviceName";
    public static final String LOGFILE_NAME_KEY = "logFileName";
    
    public static final String ROLL_FILE_TYPE_SIZE = "size";
    public static final String ROLL_FILE_TYPE_INTERVAL = "interval";
    
    public static final FlumeLogFileRollType ROLL_FILE_TYPE_DEFAULT = FlumeLogFileRollType.Size;
    public static final int ROLL_FILE_SIZE_DEFAULT = 10;
    public static final int ROLL_FILE_INTERVAL_DEFAULT = 60 * 3600;
    public static final int ROLL_FILE_MAX_OPEN_FILE = 20;
    public static final long ROLL_FILE_EXPIRY_TIMEOUT = 60 * 1000;
    public static final int ROLL_FILE_EXPIRY_CHECK_INTERVAL = 10 * 1000;
    
    public static final int MESSAGE_QUEUE_SIZE = 2000;
    
    private String rollFileRootPath;
    private FlumeLogFileRollType logFileRollType = FlumeLogFileRollType.Size;
    private int rollFileSize = ROLL_FILE_SIZE_DEFAULT;
    private int rollFileInterval = ROLL_FILE_INTERVAL_DEFAULT;
    private int rollFileMaxOpenFile = ROLL_FILE_MAX_OPEN_FILE;
    private long rollFileExpiryTimeout = ROLL_FILE_EXPIRY_TIMEOUT;
    
    /* serviceGroupName, serviceName, logFileName */
    Map<String, Map<String, Map<String, FlumeLogFileWriter>>> logFileWriterMap = new HashMap<String, Map<String, Map<String, FlumeLogFileWriter>>>();
    
    /* 排序按照log file 到期来排序的，一旦到期，就关闭日志文件 */
    TreeSet<FlumeLogFileWriter> logFileWriterSet = new TreeSet<FlumeLogFileWriter>();
    
    /* lock for logFileWriterSet */
    ReentrantLock lock = new ReentrantLock();
    
    BlockingQueue<FlumeLogFileMsg> msgQueue = new ArrayBlockingQueue<FlumeLogFileMsg>(MESSAGE_QUEUE_SIZE);
    
    Thread handleLogMsgThread = null;
    
    Thread expiryLogFileWriterThread = null;
    
    boolean isRunning = false;
    
    @Override
    public void configure(Context context)
    {
        rollFileRootPath = context.getString(ROLL_FILE_ROOT_PATH_KEY);
        Preconditions.checkArgument(rollFileRootPath != null, "Flume log file sink root path shoud not be null");
        
        String logFileRollTypeStr = context.getString(ROLL_FILE_TYPE_KEY);
        if (logFileRollTypeStr != null)
        {
            if (logFileRollTypeStr.trim().equals(ROLL_FILE_TYPE_SIZE))
            {
                logFileRollType = FlumeLogFileRollType.Size;
            }
            else if (logFileRollTypeStr.trim().equals(ROLL_FILE_TYPE_INTERVAL))
            {
                Preconditions.checkArgument(false, "Does not support interval yet: " + logFileRollTypeStr);
            }
            else
            {
                Preconditions.checkArgument(false, "Unknown roll file type: " + logFileRollTypeStr);
            }
        }
        
        rollFileSize = context.getInteger(ROLL_FILE_SIZE_KEY, ROLL_FILE_SIZE_DEFAULT);
        rollFileInterval = context.getInteger(ROLL_FILE_INTERVAL_KEY, ROLL_FILE_INTERVAL_DEFAULT);
        rollFileMaxOpenFile = context.getInteger(ROLL_FILE_MAX_OPEN_FILE_KEY, ROLL_FILE_MAX_OPEN_FILE);
        rollFileExpiryTimeout = context.getLong(ROLL_FILE_EXPIRY_TIMEOUT_KEY, ROLL_FILE_EXPIRY_TIMEOUT);
        logger.info("rollFileRootPath is: " + rollFileRootPath + "; "
                  + "logFileRollType is: " + logFileRollType + "; "
                  + "rollFileSize is: " + rollFileSize + "; "
                  + "rollFileInterval is " + rollFileInterval + "; "
                  + "rollFileMaxOpenFile is " + rollFileMaxOpenFile + "; "
                  + "rollFileExpiryTimeout is " + rollFileExpiryTimeout);
    }
    
    @Override
    public void start()
    {
        logger.info("start FlumeLogFileSink ...");
        
        isRunning = true;
        
        handleLogMsgThread = new Thread(new Runnable() {

            @Override
            public void run()
            {
                handleLogMsg();
            }
            
        });
        
        expiryLogFileWriterThread = new Thread(new Runnable() {

            @Override
            public void run()
            {
                expiryLogFileWriter();
            }
            
        });
        
        handleLogMsgThread.start();
        expiryLogFileWriterThread.start();
    }
    
    @Override
    public void stop()
    {
        logger.info("end FlumeLogFileSink ...");
        
        isRunning = false;
        handleLogMsgThread.interrupt();
        expiryLogFileWriterThread.interrupt();
        
        for(FlumeLogFileWriter writer : logFileWriterSet)
        {
            writer.stop();
        }
        logFileWriterSet.clear();
        logFileWriterMap.clear();
    }
    
    @Override
    public Status process() throws EventDeliveryException
    {
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try 
        {
            Event event = ch.take();
            if (event != null)
            {
                writeEvent(event);
            }
            txn.commit();
            return Status.READY ;
        }
        finally 
        {
            txn.close();
        }
    }
    
    private void handleLogMsg()
    {
        logger.info("start handleLogMsg ...");
        while(isRunning)
        {
            try
            {
                FlumeLogFileMsg msg = msgQueue.take();
                boolean ret = writeMsg(msg);
                if (!ret)
                {
                    logger.error("Fail to write the log: " + msg);
                }
            } 
            catch (InterruptedException e)
            {
                if (!isRunning)
                {
                    logger.info("The main thread stop");
                }
                else
                {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private boolean writeMsg(FlumeLogFileMsg msg)
    {
        lock.lock();
        try
        {
            String serviceGroupName = msg.getServiceGroupName();
            String serviceName = msg.getServiceName();
            String logFileName = msg.getLogFileName();
            byte[] logMsg = msg.getMessage();
            if (serviceGroupName == null || serviceName == null || logFileName == null || logMsg == null)
            {
                logger.error("serviceGroupName: " + serviceGroupName + "; serviceName: " + serviceName + "; logFileName: " + logFileName + "; logMsg: " + logMsg);
                return false;
            }
            Map<String, Map<String, FlumeLogFileWriter>> serviceNameToFileNameMap = logFileWriterMap.get(serviceGroupName);
            if (serviceNameToFileNameMap == null)
            {
                serviceNameToFileNameMap = new HashMap<String, Map<String, FlumeLogFileWriter>>();
                logFileWriterMap.put(serviceGroupName, serviceNameToFileNameMap);
            }
            Map<String, FlumeLogFileWriter> fileNameToWriterMap = serviceNameToFileNameMap.get(serviceName);
            if (fileNameToWriterMap == null)
            {
                fileNameToWriterMap = new HashMap<String, FlumeLogFileWriter>();
                serviceNameToFileNameMap.put(serviceName, fileNameToWriterMap);
            }
            FlumeLogFileWriter writer = fileNameToWriterMap.get(logFileName);
            if (writer == null)
            {
                writer = new FlumeLogFileWriter(rollFileRootPath, serviceGroupName, serviceName, logFileName, rollFileSize);
                logger.info("creat a writer " + writer);
                writer.start();
                addFlumeLogFileWriter(writer);
                fileNameToWriterMap.put(logFileName, writer);
            }
            
            return writeMsg(writer, logMsg);
        }
        finally
        {
            lock.unlock();
        }
    }

    private void addFlumeLogFileWriter(FlumeLogFileWriter writer)
    {
        while (logFileWriterSet.size() >= rollFileMaxOpenFile)
        {
            FlumeLogFileWriter discardWriter = logFileWriterSet.pollFirst();
            discardWriter.stop();
            removeWriterFromlogFileWriterMap(discardWriter);
            logger.info("Discard writer: " + discardWriter);
        }
        logger.info("Add writer to logFileWriterSet " + writer);
        logFileWriterSet.add(writer);
    }
    
    private boolean writeMsg(FlumeLogFileWriter writer, byte[] msg)
    {
        if (writer == null)
        {
            logger.error("writer should not be null");
            return false;
        }
        if (msg == null)
        {
            logger.error("log message should not be null. The writer is: " + writer);
            return false;
        }
        if (!logFileWriterSet.contains(writer))
        {
            logger.error("logFileWriterSet does not contain the writer: " + writer + "; logFileWriterSet is: " + logFileWriterSet + "; logFileWriterMap is: " + logFileWriterMap);
            return false;
        }

        logFileWriterSet.remove(writer);
        boolean ret = writer.write(msg);
        logFileWriterSet.add(writer);
        return ret;
    }
    
    /* 这里要与writeMsg方法互斥 */
    private void expiryLogFileWriter()
    {
        while(isRunning)
        {
            lock.lock();
            try
            {
                while(logFileWriterSet.size() > 0)
                {
                    FlumeLogFileWriter writer = logFileWriterSet.first();
                    long changeTimestamp = writer.getChangeTimeStamp();
                    if (System.currentTimeMillis() - changeTimestamp > rollFileExpiryTimeout)
                    {
                        logFileWriterSet.remove(writer);
                        writer.stop();
                        removeWriterFromlogFileWriterMap(writer);
                        logger.info("remove the expiry writer: " + writer);
                    }
                    else
                    {
                        break;
                    }
                }
            }
            finally
            {
                lock.unlock();
            }
            try
            {
                Thread.sleep(ROLL_FILE_EXPIRY_CHECK_INTERVAL);
            } 
            catch (InterruptedException e)
            {
                if(!isRunning)
                {
                    logger.info("FlumeLogFileSink is stopped");
                }
                else
                {
                    e.printStackTrace();
                }
            }
        }
    }
    
    private void writeEvent(Event event)
    {
        if (event == null)
        {
            logger.error("event shoud not be empty");
            return;
        }
        Map<String, String> headerMap = event.getHeaders();
        String serviceGroupName = headerMap.get(SERVICEGROUP_NAME_KEY);
        if (serviceGroupName == null)
        {
            logger.error("serviceGroupName should not empty in event: " + event);
            return;
        }
        String serviceName = headerMap.get(SERVICE_NAME_KEY);
        if (serviceName == null)
        {
            logger.error("serviceName should not empty in event: " + event);
            return;
        }
        String logFileName = headerMap.get(LOGFILE_NAME_KEY);
        if (logFileName == null)
        {
            logger.error("logFileName should not empty in event: " + event);
            return;
        }
        byte[] logMsg = event.getBody();
        FlumeLogFileMsg flumeLogFileMsg = new FlumeLogFileMsg(serviceGroupName, serviceName, logFileName, logMsg);
        boolean ret = msgQueue.offer(flumeLogFileMsg);
        if (ret)
        {
            logger.debug("Add the msg to msgQueue: " + flumeLogFileMsg);
        }
        else
        {
            logger.error("Fail to add msg to msgQueue: " + flumeLogFileMsg);
        }
    }
    
    private void removeWriterFromlogFileWriterMap(FlumeLogFileWriter writer)
    {
        String serviceGroupName = writer.getServiceGroupName();
        Map<String, Map<String, FlumeLogFileWriter>> serviceNameToFileNameMap = logFileWriterMap.get(serviceGroupName);
        if (serviceNameToFileNameMap == null)
        {
            logger.error("Can not find serviceGroup in logFileWriterMap " + serviceGroupName);
            return;
        }
        String serviceName = writer.getServiceName();
        Map<String, FlumeLogFileWriter> fileNameToWriterMap = serviceNameToFileNameMap.get(serviceName);
        if (fileNameToWriterMap == null)
        {
            logger.error("Can not find service in serviceNameToFileNameMap " + serviceName);
            return;
        }
        String logFileName = writer.getLogFileName();
        if (!fileNameToWriterMap.containsKey(logFileName))
        {
            logger.error("Can not find log file in fileNameToWriterMap " + logFileName);
            return;
        }
        
        fileNameToWriterMap.remove(logFileName);
        logger.info("remove the writer from logFileWriterMap " + writer);
        if (fileNameToWriterMap.size() == 0)
        {
            serviceNameToFileNameMap.remove(serviceName);
        }
        if (serviceNameToFileNameMap.size() == 0)
        {
            logFileWriterMap.remove(serviceGroupName);
        }
    }
}
