package com.riptano.datameer.importjob;

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import datameer.dap.sdk.datastore.DataStoreModel;
import datameer.dap.sdk.entity.DataStore;

/**
 * Sets up the connection information used to connect with Cassandra cluster
 * 
 * @author zznate <nate@riptano.com>
 *
 */
public class CassandraDataStoreModel implements DataStoreModel {

    private String host;
    private int port;
    private boolean framed;
    
    
    // constructor taking dataStore
    public CassandraDataStoreModel(DataStore dataStore) {
        host = dataStore.getStringProperty("host", "localhost");
        port = dataStore.getIntProperty("port", 9160);
        framed = dataStore.getBooleanProperty("useFramedTransport", false);        
    }
    
    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public void setupConf(Configuration conf) {
        // how do I take input from the page and apply it to Configuration?
        // here we can *WRITE TO* hadoop configuration
        ConfigHelper.setThriftContact(conf, host, port);
        conf.setBoolean(CassandraConnectionUtils.THRIFT_FRAMED_TRANSPORT, framed);
    }

    @Override
    public void testConnect() throws InterruptedException {
        
        Client client;
        try {
            client = CassandraConnectionUtils.createConnection(host, port, framed);
            client.describe_version();
        } catch (IOException ioe) {
            // This happens if we cannot connect to the cluster. CCU.createConnections builds details.
            throw new RuntimeException(ioe.getMessage(), ioe);
        } catch (TException e) {
            // This would be odd and most likely indicate a Thrift version mismatch or Framed/Non-Framed transport issue
            throw new RuntimeException("Could not complete describe_version operation against Apache Cassandra. " +
                    "Check Thrift versions and Framed/Non-Framed transport configurations?" + e.getMessage(), e);
        } 
    }
        

}
