package com.riptano.datameer.importjob;

import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;

public class CassandraRowRecord {

    private static final String MSG_FORMAT = "CassandraRowRecord[key=%s,cols=%s]";
    private final String key;
    private final SortedMap<byte[], IColumn> rows;
    private final AbstractType comparator, subComparator;
    private final IPartitioner partitioner;
    
    public CassandraRowRecord(String key, SortedMap<byte[], IColumn> rows, 
            AbstractType comparator, AbstractType subComparator, IPartitioner partitioner) {
        this.key = key;
        this.rows = rows;
        this.comparator = comparator;
        this.subComparator = subComparator;
        this.partitioner = partitioner;        
    }
    
    public String getKey() {
        return key;
    }
    
    public Map<byte[], IColumn> getRows() {
        return rows;
    }
    
    public String getColumnValueAsString(byte[] columnName) {        
        return comparator.getString(columnName);        
    }
    
    @Override
    public String toString() {
        return String.format(MSG_FORMAT, key, rows != null ? rows.toString() : "<no rows found>");
    }
}
