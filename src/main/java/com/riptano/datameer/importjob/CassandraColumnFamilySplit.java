package com.riptano.datameer.importjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapred.InputSplit;

/**
 * An implementation of InputSplit that uses a token range and an array of 
 * nodes for providing InputSplit objects. Modeled off of ColumnFamilySplit
 * but backwards compatible with old-style Hadoop InputSplit
 * 
 * @author zznate <nate@riptano.com>
 */
public class CassandraColumnFamilySplit implements InputSplit {
    
    private String startToken;
    private String endToken;
    private String[] dataNodes;

    public CassandraColumnFamilySplit(String startToken, String endToken, String[] dataNodes)
    {
        assert startToken != null;
        assert endToken != null;
        this.startToken = startToken;
        this.endToken = endToken;
        this.dataNodes = dataNodes;
    }

    
    @Override
    public long getLength() throws IOException {
        // not really available/germaine in Cassandra view of things
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        return dataNodes;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        startToken = in.readUTF();
        endToken = in.readUTF();

        int numOfEndpoints = in.readInt();
        dataNodes = new String[numOfEndpoints];
        for(int i = 0; i < numOfEndpoints; i++)
        {
            dataNodes[i] = in.readUTF();
        }

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(startToken);
        out.writeUTF(endToken);

        out.writeInt(dataNodes.length);
        for (String endpoint : dataNodes)
        {
            out.writeUTF(endpoint);
        }
    }

    @Override
    public String toString()
    {
        return "CassandraColumnFamilySplit{" +
               "startToken='" + startToken + '\'' +
               ", endToken='" + endToken + '\'' +
               ", dataNodes=" + (dataNodes == null ? null : Arrays.asList(dataNodes)) +
               '}';
    }
}
