package com.riptano.datameer.importjob;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.collect.AbstractIterator;

import datameer.dap.sdk.importjob.RecordSourceReader;

/**
 * Loosely based on o.a.c.h.ColumnFamilyRecordReader in the 0.7.x version of
 * Apache Cassandra. 
 * 
 * @author zznate <nate@riptano.com>
 *
 */
public class CassandraColumnFamilyRecordSourceReader implements RecordSourceReader<CassandraRowRecord> {
    
    private static Logger log = Logger.getLogger(CassandraColumnFamilyRecordSourceReader.class);

    private CassandraDataImportJobModel dataImportJobModel;
    private CassandraColumnFamilySplit columnFamilySplit;
    private Configuration configuration;
    private RowIterator rowIterator;
    
    public CassandraColumnFamilyRecordSourceReader(CassandraDataImportJobModel dataImportJobModel,
            CassandraColumnFamilySplit columnFamilySplit,
            Configuration configuration) {
        this.dataImportJobModel = dataImportJobModel;
        this.columnFamilySplit = columnFamilySplit;
        this.configuration = configuration;
        rowIterator = new RowIterator();
    }
    
    @Override
    public void close() throws IOException {
        // TODO add a closeConnection here and in CassandraConnectionUtils
        
    }

    @Override
    public long getPos() throws IOException {
        return rowIterator.rowsRead() / dataImportJobModel.getBatchCount();
    }

    @Override
    public CassandraRowRecord readNext() throws IOException {        
        if ( !rowIterator.hasNext() ){
            return null;
        }
        Pair<String, SortedMap<byte[], IColumn>> next = rowIterator.next();
        if ( log.isDebugEnabled() ) {
            log.debug("Read row: " + next);
        }
        if ( next == null ) {
           return null; 
        }
        return new CassandraRowRecord(next.left, next.right);        
    }
    
    
    private class RowIterator extends AbstractIterator<Pair<String, SortedMap<byte[], IColumn>>> {
        private List<KeySlice> rows;
        private String startToken;
        private int totalRead = 0;
        private int i = 0;
        private final AbstractType comparator;
        private final AbstractType subComparator;
        private final IPartitioner partitioner;
        private Cassandra.Client client;

        private RowIterator() {           
            try {
                client = CassandraConnectionUtils.createConnection(dataImportJobModel);

                partitioner = FBUtilities.newPartitioner(client.describe_partitioner());

                Map<String, String> info = client.describe_keyspace(dataImportJobModel.getKeyspace())
                .get(dataImportJobModel.getColumnFamily());
                
                comparator = FBUtilities.getComparator(info.get("CompareWith"));
                subComparator = FBUtilities.getComparator(info.get("CompareSubcolumnsWith"));
            } catch (TException e) {
                throw new RuntimeException("error communicating via Thrift", e);
            } catch (NotFoundException e) {
                throw new RuntimeException("server reports no such keyspace " 
                        + dataImportJobModel.getKeyspace(), e);
            } catch (Exception ex) {
                throw new RuntimeException("Problem connecting to Apache Cassandra: " + ex.getMessage(), ex);
            }
        }

        private void maybeInit() {
            // check if we need another batch 
            if (rows != null && i >= rows.size())
                rows = null;
            
            if (rows != null)
                return;
            if ( log.isDebugEnabled() ) {
                log.debug("Working ColumnFamilySplit: " + columnFamilySplit);
            }
            if (startToken == null) {
                startToken = columnFamilySplit.getStartToken();
            } 
            else if (startToken.equals(columnFamilySplit.getEndToken())) {
                rows = null;
                return;
            }

            KeyRange keyRange = new KeyRange(dataImportJobModel.getBatchCount())
                                .setStart_token(startToken)
                                .setEnd_token(columnFamilySplit.getEndToken());

            if ( log.isDebugEnabled() ) {
                log.debug("Using KeyRange: " + keyRange);
            }

            try {

                rows = client.get_range_slices(dataImportJobModel.getKeyspace(),
                                               new ColumnParent(dataImportJobModel.getColumnFamily()),
                                               dataImportJobModel.getSlicePredicate(),
                                               keyRange,
                                               ConsistencyLevel.ONE);
                    
                // nothing new? reached the end
                if (rows.isEmpty()) {
                    rows = null;
                    return;
                }
                               
                // reset to iterate through this new batch
                i = 0;
                
                // prepare for the next slice to be read
                KeySlice lastRow = rows.get(rows.size() - 1);
                startToken = partitioner.getTokenFactory().toString(partitioner.getToken(lastRow.getKey()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


        /**
         * @return total number of rows read by this record reader
         */
        public int rowsRead() {
            return totalRead;
        }


        protected Pair<String, SortedMap<byte[], IColumn>> computeNext() {
            maybeInit();
            if (rows == null)
                return null;
            
            totalRead++;
            KeySlice ks = rows.get(i++);
            SortedMap<byte[], IColumn> map = new TreeMap<byte[], IColumn>(comparator);
            for (ColumnOrSuperColumn cosc : ks.columns) {
                IColumn column = unthriftify(cosc);
                map.put(column.name(), column);
            }
            return new Pair<String, SortedMap<byte[], IColumn>>(ks.key, map);
        }
        


        private IColumn unthriftify(ColumnOrSuperColumn cosc) {
            if (cosc.column == null)
                return unthriftifySuper(cosc.super_column);
            return unthriftifySimple(cosc.column);
        }

        private IColumn unthriftifySuper(SuperColumn super_column) {
            org.apache.cassandra.db.SuperColumn sc = new org.apache.cassandra.db.SuperColumn(super_column.name, subComparator);
            for (Column column : super_column.columns) {
                sc.addColumn(unthriftifySimple(column));
            }
            return sc;
        }

        private IColumn unthriftifySimple(Column column) {
            return new org.apache.cassandra.db.Column(column.name, column.value, column.timestamp);
        }
    }


}
