package com.riptano.datameer.importjob;

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class CassandraConnectionUtils {
    
    public static final String THRIFT_FRAMED_TRANSPORT = "use_thrift_framed_transport";
    
    public static Cassandra.Client createConnection(Configuration conf) throws IOException {
        return createConnection(ConfigHelper.getInitialAddress(conf), 
                    ConfigHelper.getThriftPort(conf), false);
    }
    
    public static Cassandra.Client createConnection(String host, Integer port, boolean framed) throws IOException {
        TSocket socket = new TSocket(host, port, 5000);
        TTransport trans = framed ? new TFramedTransport(socket) : socket;
        try {
            trans.open();
        }
        catch (TTransportException e) {
            throw new IOException(String.format(ERR_MSG, new Object[]{host, port, e.getMessage()}), e);
        }
        return new Cassandra.Client(new TBinaryProtocol(trans));
    }
    
    private static final String ERR_MSG = "Cannot connect to Apache Cassandra. Host: %s Port: %d \nReason: %s";
}
