package com.riptano.datameer.importjob;

import java.util.Map;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;

public class CassandraRowRecord {

    private final String key;
    private final SortedMap<byte[], IColumn> rows;
    
    public CassandraRowRecord(String key, SortedMap<byte[], IColumn> rows) {
        this.key = key;
        this.rows = rows;
    }
    
    public String getKey() {
        return key;
    }
    
    public Map<byte[], IColumn> getRows() {
        return rows;
    }
    
    @Override
    public String toString() {
        return String.format("CassandraRowRecord[key=%s,cols=%s]", key, rows != null ? rows.toString() : "<no rows found>");
    }
}
