package com.riptano.datameer.importjob;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import datameer.dap.sdk.job.Splitter;

public class CassandraColumnFamilySplitter implements Splitter<CassandraColumnFamilySplit> {

    private static Logger log = Logger.getLogger(CassandraColumnFamilySplit.class);
    
    private String keyspace;
    private String cfName;
    
    @Override
    public CassandraColumnFamilySplit[] createPreviewSplits(Configuration arg0, int arg1) throws IOException {

        return null;
    }

    @Override
    public CassandraColumnFamilySplit[] createSplits(Configuration conf) throws IOException {
        // this is the "noodle the tokens" portion
        // just like getSplits() on CFIF
        // here is where we will hand off the Configuration
        // cannonical ranges and nodes holding replicas
        List<TokenRange> masterRangeNodes = getRangeMap(conf);

        keyspace = ConfigHelper.getKeyspace(conf);
        cfName = ConfigHelper.getColumnFamily(conf);
        
        // cannonical ranges, split into pieces, fetching the splits in parallel 
        ExecutorService executor = Executors.newCachedThreadPool();
        List<InputSplit> splits = new ArrayList<InputSplit>();

        try {
            List<Future<List<InputSplit>>> splitfutures = new ArrayList<Future<List<InputSplit>>>();
            for (TokenRange range : masterRangeNodes) {
                // for each range, pick a live owner and ask it to compute bite-sized splits
                splitfutures.add(executor.submit(new SplitCallable(range, conf)));
            }
    
            // wait until we have all the results back
            for (Future<List<InputSplit>> futureInputSplits : splitfutures) {
                try {
                    splits.addAll(futureInputSplits.get());
                } 
                catch (Exception e) {
                    throw new IOException("Could not get input splits", e);
                } 
            }
        } 
        finally {
            executor.shutdownNow();
        }

        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return null;
    }

    @Override
    public CassandraColumnFamilySplit[] resolveCombinedSplits(CassandraColumnFamilySplit arg0) throws IOException {

        return null;
    }
    
    private List<TokenRange> getRangeMap(Configuration conf) throws IOException
    {
        Cassandra.Client client = 
            CassandraConnectionUtils.createConnection(ConfigHelper.getInitialAddress(conf), 
                    ConfigHelper.getThriftPort(conf), true);

        List<TokenRange> map;
        try {
            map = client.describe_ring(ConfigHelper.getKeyspace(conf));
        } catch (org.apache.thrift.TException e) {
            throw new RuntimeException(e);
        }
        catch (InvalidRequestException e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    private List<String> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf) throws IOException
    {
        // TODO handle failure of range replicas & retry
        List<String> splits;
        int splitsize = ConfigHelper.getInputSplitSize(conf);
        try {
            Cassandra.Client client = 
                CassandraConnectionUtils.createConnection(range.endpoints.get(0), ConfigHelper.getThriftPort(conf), true);
            
            splits = client.describe_splits(range.start_token, range.end_token, splitsize);
        } catch (TException e) {
            throw new RuntimeException(e);
        }        
        return splits;
    }

    
    class SplitCallable implements Callable<List<InputSplit>> {

        private final TokenRange range;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Configuration conf) {
            this.range = tr;
            this.conf = conf;
        }

        public List<InputSplit> call() throws Exception
        {
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            List<String> tokens = getSubSplits(keyspace, cfName, range, conf);

            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            for (int i = 0; i < endpoints.length; i++) {
                endpoints[i] = InetAddress.getByName(endpoints[i]).getHostName();
            }
            
            for (int i = 1; i < tokens.size(); i++) {
                ColumnFamilySplit split = new ColumnFamilySplit(tokens.get(i - 1), tokens.get(i), endpoints);
                splits.add(split);
            }
            return splits;
        }
    }

}
