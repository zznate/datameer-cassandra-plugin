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
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import datameer.dap.sdk.importjob.Splitter;

public class CassandraColumnFamilySplitter implements Splitter<CassandraColumnFamilySplit> {

    private static Logger log = Logger.getLogger(CassandraColumnFamilySplit.class);
    
    private CassandraDataImportJobModel cassandraDataImportJobModel;
    
    public CassandraColumnFamilySplitter(CassandraDataImportJobModel importJobModel) {
        cassandraDataImportJobModel = importJobModel;
    }
    
    @Override
    public CassandraColumnFamilySplit[] createPreviewSplits(Configuration arg0, int arg1) throws IOException {                
        List<TokenRange> masterRangeNodes = getRangeMap(arg0);
        List<CassandraColumnFamilySplit> splits = new ArrayList<CassandraColumnFamilySplit>(masterRangeNodes.size());
        for (TokenRange tokenRange : masterRangeNodes) {
            splits.add(new CassandraColumnFamilySplit(tokenRange.start_token, tokenRange.end_token, tokenRange.endpoints.toArray(new String[]{})));
        }

        return splits.toArray(new CassandraColumnFamilySplit[]{});
    }

    @Override
    public CassandraColumnFamilySplit[] createSplits(Splitter.SplitHint splitHint) throws IOException {
        // this is the "noodle the tokens" portion
        // just like getSplits() on CFIF
        // here is where we will hand off the Configuration
        // cannonical ranges and nodes holding replicas
        Configuration conf = splitHint.getConf();
        List<TokenRange> masterRangeNodes = getRangeMap(conf);
        
        // cannonical ranges, split into pieces, fetching the splits in parallel 
        ExecutorService executor = Executors.newCachedThreadPool();
        List<CassandraColumnFamilySplit> splits = new ArrayList<CassandraColumnFamilySplit>();

        try {
            List<Future<List<CassandraColumnFamilySplit>>> splitfutures = new ArrayList<Future<List<CassandraColumnFamilySplit>>>();
            for (TokenRange range : masterRangeNodes) {
                // for each range, pick a live owner and ask it to compute bite-sized splits
                splitfutures.add(executor.submit(new SplitCallable(range, conf)));
            }
    
            // wait until we have all the results back
            for (Future<List<CassandraColumnFamilySplit>> futureInputSplits : splitfutures) {
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
        return splits.toArray(new CassandraColumnFamilySplit[]{});
    }

    
    private List<TokenRange> getRangeMap(Configuration conf) throws IOException
    {
        Cassandra.Client client = 
            CassandraConnectionUtils.createConnection(cassandraDataImportJobModel);

        List<TokenRange> map;
        try {
            map = client.describe_ring(cassandraDataImportJobModel.getKeyspace());
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
                CassandraConnectionUtils.createConnection(cassandraDataImportJobModel);
                //CassandraConnectionUtils.createConnection(range.endpoints.get(0), ConfigHelper.getThriftPort(conf), false);
                // TODO add config parameter for thrift == gossip interface
            splits = client.describe_splits(range.start_token, range.end_token, splitsize);
        } catch (TException e) {
            throw new RuntimeException(e);
        }        
        return splits;
    }

    
    class SplitCallable implements Callable<List<CassandraColumnFamilySplit>> {

        private final TokenRange range;
        private final Configuration conf;

        public SplitCallable(TokenRange tr, Configuration conf) {
            this.range = tr;
            this.conf = conf;
        }

        public List<CassandraColumnFamilySplit> call() throws Exception
        {            
            List<String> tokens = getSubSplits(cassandraDataImportJobModel.getKeyspace(), 
                    cassandraDataImportJobModel.getColumnFamily(), range, conf);

            // turn the sub-ranges into InputSplits
            String[] endpoints = range.endpoints.toArray(new String[range.endpoints.size()]);
            // hadoop needs hostname, not ip
            for (int i = 0; i < endpoints.length; i++) {
                endpoints[i] = InetAddress.getByName(endpoints[i]).getHostName();
            }
            
            return buildSplits(tokens, endpoints);
        }
    }
    
    private List<CassandraColumnFamilySplit> buildSplits(List<String> tokens, String[] endpoints) {
        ArrayList<CassandraColumnFamilySplit> splits = new ArrayList<CassandraColumnFamilySplit>();
        for (int i = 1; i < tokens.size(); i++) {
            CassandraColumnFamilySplit split = new CassandraColumnFamilySplit(tokens.get(i - 1), tokens.get(i), endpoints);
            splits.add(split);
        }
        return splits;
    }

    
}
