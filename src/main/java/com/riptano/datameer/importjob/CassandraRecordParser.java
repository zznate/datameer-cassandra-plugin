package com.riptano.datameer.importjob;

import org.apache.hadoop.mapred.InputSplit;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.common.Record;
import datameer.dap.sdk.common.RecordCollector;
import datameer.dap.sdk.importjob.AbstractRecordParser;

public class CassandraRecordParser extends AbstractRecordParser<CassandraRowRecord> {
    
    public CassandraRecordParser(Field[] fields) {
        super(fields);
    }

    @Override
    public void initSplit(InputSplit arg0) {
        // noop
    }

    @Override
    public void parse(RecordCollector arg0, CassandraRowRecord arg1) throws Exception {
        int index = 0;
        Object[] values = new Object[getIncludedFields().length];
        for (Field field : getIncludedFields()) {
            String origin = field.getOrigin();
            if (origin.equals("01")) {
                values[index++] = arg1.getKey();
            }
        }
        arg0.collect(new Record(getIncludedFieldTypes(), values));        
    }
    
    

}
