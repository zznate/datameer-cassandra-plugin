package com.riptano.datameer.importjob;

import java.io.IOException;

import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.TException;

import datameer.dap.sdk.datastore.DataStoreModel;
import datameer.dap.sdk.entity.DataStore;

/**
 * Sets up the connection information used to connect with Cassandra cluster.
 * 
 * @author zznate <nate@riptano.com>
 *
 */
public class CassandraDataStoreModel implements DataStoreModel {
    
    private static final long serialVersionUID = -4448985475836434591L;
    
    private CassandraHostConfigurator cassandraHostConfigurator;
    private boolean preserveLocality;
    
    static final String PRESERVE_LOCALITY = "cassandra.preserveLocality";
    static final String USE_FRAMED = "cassandra.useFramed";
    static final String HOSTS = "cassandra.hosts";
    
    public CassandraDataStoreModel(DataStore dataStore) {
        cassandraHostConfigurator = new CassandraHostConfigurator(dataStore.getStringProperty(HOSTS, "localhost:9160"));               
        cassandraHostConfigurator.setUseThriftFramedTransport(dataStore.getBooleanProperty(USE_FRAMED, false));        
        preserveLocality = dataStore.getBooleanProperty(PRESERVE_LOCALITY, false);
    }
        
    
    public CassandraHostConfigurator getCassandraHostConfigurator() {
        return cassandraHostConfigurator;
    }

    public boolean getPreserveLocality() {
        return preserveLocality;
    }

    @Override
    public boolean isLocal() {
        return false;
    }

    @Override
    public void setupConf(Configuration conf) {
        // no-op. We pass around data via datameer's conf structure
    }

    @Override
    public void testConnect() throws InterruptedException {
        Client client;
        
        try {
            CassandraHost[] cassandraHosts = cassandraHostConfigurator.buildCassandraHosts();
            client = CassandraConnectionUtils.createConnection(cassandraHosts[0]);
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
