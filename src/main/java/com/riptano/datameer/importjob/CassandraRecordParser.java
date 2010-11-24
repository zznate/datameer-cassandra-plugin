package com.riptano.datameer.importjob;

import java.util.List;

import org.apache.cassandra.db.IColumn;
import org.apache.hadoop.mapred.InputSplit;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.common.Record;
import datameer.dap.sdk.common.RecordCollector;
import datameer.dap.sdk.importjob.AbstractRecordParser;

public class CassandraRecordParser extends AbstractRecordParser<CassandraRowRecord> {
    
    private CassandraDataImportJobModel dataImportJobModel;
    
    public CassandraRecordParser(Field[] fields, CassandraDataImportJobModel dataImportJobModel) {
        super(fields);
        this.dataImportJobModel = dataImportJobModel;
    }

    @Override
    public void initSplit(InputSplit arg0) {
        // noop
    }

    @Override
    public void parse(RecordCollector arg0, CassandraRowRecord arg1) throws Exception {
        int index = 0;
        List<String> columns = dataImportJobModel.getColumnNames();
        Object[] values = new Object[getIncludedFields().length];
        for (Field field : getIncludedFields()) {
            String origin = field.getOrigin();
            if ( origin.equals(CassandraRowRecordSchemaDector.KEY_FIELD)) {
                values[index++] = arg1.getKey();
            } else if (columns.contains(origin)) {
                values[index++] = new String(arg1.getRows().get(origin.getBytes("UTF-8")).value(), "UTF-8");                
            }
        }
        arg0.collect(new Record(getIncludedFieldTypes(), values));        
    }
    
    

}
