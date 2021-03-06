package yanbinwa.flumeLogFileSink;

import java.util.TreeSet;

import org.junit.Test;

public class FlumeLogFileWriterTest
{

    @Test
    public void test()
    {
        String rootPath = "/Users/yanbinwa/Documents/workspace/springboot/serviceManager/flumeLogFileSink/test";
        String serviceGroupName = "collection";
        String serviceName = "collection_active";
        String logFileName = "message";
        int rollFileSize = 1000;
        FlumeLogFileWriter writer = new FlumeLogFileWriter(rootPath, serviceGroupName, serviceName, logFileName, rollFileSize);
        writer.start();
        for (int i = 0; i < 1; i ++)
        {
            writer.write("Wyblpwan".getBytes());
            writer.write("123123123123\n".getBytes());
        }
        
        writer.stop();
    }
    
    @Test
    public void treeSetTest()
    {
        TreeSet<FlumeLogFileWriter> logFileWriterSet = new TreeSet<FlumeLogFileWriter>();
        String rootPath = "/Users/yanbinwa/Documents/workspace/springboot/serviceManager/flumeLogFileSink/test";
        String serviceGroupName = "collection";
        String serviceName = "collection_active";
        String logFileName = "message";
        int rollFileSize = 1000;
        FlumeLogFileWriter writer1 = new FlumeLogFileWriter(rootPath, serviceGroupName, serviceName, logFileName, rollFileSize);
        writer1.start();
        logFileWriterSet.add(writer1);
        try
        {
            Thread.sleep(100);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        
        serviceName = "collection_standby";
        FlumeLogFileWriter writer2 = new FlumeLogFileWriter(rootPath, serviceGroupName, serviceName, logFileName, rollFileSize);
        writer2.start();
        logFileWriterSet.add(writer2);
        System.out.println(logFileWriterSet.first());
        
        try
        {
            Thread.sleep(100);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        
        logFileWriterSet.remove(writer1);
        writer1.write("123".getBytes());
        logFileWriterSet.add(writer1);
        
        System.out.println(logFileWriterSet.first());
        
        try
        {
            Thread.sleep(100);
        } 
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        
        logFileWriterSet.remove(writer2);
        writer2.write("wyb".getBytes());
        logFileWriterSet.add(writer2);
        
        System.out.println(logFileWriterSet.first());
        
    }
    
    @Test
    public void testTreeSetContain()
    {
        TreeSet<FlumeLogFileWriter> logFileWriterSet = new TreeSet<FlumeLogFileWriter>();
        String rootPath = "/Users/yanbinwa/Documents/workspace/springboot/serviceManager/flumeLogFileSink/test";
        String serviceGroupName = "collection";
        String serviceName = "collection_active";
        String logFileName = "message";
        int rollFileSize = 1000;
        FlumeLogFileWriter writer1 = new FlumeLogFileWriter(rootPath, serviceGroupName, serviceName, logFileName, rollFileSize);
        
        logFileWriterSet.add(writer1);
        
        //serviceGroupName = "cache";
        FlumeLogFileWriter writer2 = new FlumeLogFileWriter(rootPath, serviceGroupName, serviceName, logFileName, rollFileSize);
        writer2.setChangeTimeStamp(1000L);
        
        System.out.println("--------------------------");
        System.out.println(logFileWriterSet.size());
        
        if (logFileWriterSet.contains(writer1))
        {
            System.out.println("Contained writer1");
        }
        
        if (logFileWriterSet.contains(writer2))
        {
            System.out.println("Contained writer2");
        }
        
        FlumeLogFileWriter writer = logFileWriterSet.first();
        System.out.println(writer.getChangeTimeStamp());
        
        TreeSet<String> stringSet = new TreeSet<String>();
        stringSet.add("wyb");
        stringSet.add("123");
        System.out.println("--------------------------");
        System.out.println(stringSet.size());
    }

}
