package com.riptano.datameer.importjob;

import java.io.IOException;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import datameer.dap.sdk.datastore.DataStoreModel;
import datameer.dap.sdk.entity.DataStore;
import datameer.dap.sdk.widget.WizardPageDefinition;

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
    
    
    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public void setupConf(Configuration conf) {
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
            ioe.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public WizardPageDefinition createDetailsWizardPage() {

        return null;
    }

    @Override
    public void initFrom(DataStore dataStore) {
        // has all the connection information properties
        // host ip, port
        host = dataStore.getStringProperty("host", null);
        port = dataStore.getIntProperty("port", 9160);
        framed = dataStore.getBooleanProperty("useFramedTransport", true);
    }

    @Override
    public void writeTo(DataStore dataStore) {

        
    }
    
        

}
