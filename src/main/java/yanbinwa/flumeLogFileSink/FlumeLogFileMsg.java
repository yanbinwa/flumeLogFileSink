package yanbinwa.flumeLogFileSink;

public class FlumeLogFileMsg
{
    private String serviceGroupName;
    
    private String serviceName;
    
    private String logFileName;
    
    private byte[] message;
    
    public FlumeLogFileMsg(String serviceGroupName, String serviceName, String logFileName, byte[] message)
    {
        this.serviceGroupName = serviceGroupName;
        this.serviceName = serviceName;
        this.logFileName = logFileName;
        this.message = message;
    }
    
    public String getServiceGroupName()
    {
        return this.serviceGroupName;
    }
    
    public void setServiceGroupName(String serviceGroupName)
    {
        this.serviceGroupName = serviceGroupName;
    }
    
    public String getServiceName()
    {
        return this.serviceName;
    }
    
    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }
    
    public String getLogFileName()
    {
        return this.logFileName;
    }
    
    public void setLogFileName(String logFileName)
    {
        this.logFileName = logFileName;
    }
    
    public byte[] getMessage()
    {
        return this.message;
    }
    
    public void setMessage(byte[] message)
    {
        this.message = message;
    }
    
    @Override
    public String toString()
    {
        String msg = "serviceGroupName is: " + serviceGroupName + "; "
                   + "serviceName is: " + serviceName + "; "
                   + "logFileName is: " + logFileName + "; "
                   + "message is: " + new String(message);
        return msg;
    }
}
