package com.riptano.datameer.importjob;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.common.RecordCollector;
import datameer.dap.sdk.importjob.AbstractImportFormat;
import datameer.dap.sdk.importjob.ImportJobModel;
import datameer.dap.sdk.importjob.MapBasedRecordParser;
import datameer.dap.sdk.importjob.MapBasedRecordSchemaDetector;
import datameer.dap.sdk.importjob.MapParser;
import datameer.dap.sdk.importjob.NoDataRecordSchemaDetector;
import datameer.dap.sdk.importjob.RecordParser;
import datameer.dap.sdk.importjob.RecordSchemaDetector;
import datameer.dap.sdk.importjob.RecordSourceReader;
import datameer.dap.sdk.importjob.TextFieldAnalyzer;
import datameer.dap.sdk.job.Splitter;

public class CassandraColumnFamilyInputFormat extends AbstractImportFormat<CassandraRowRecord> {

    private CassandraDataImportJobModel importJobModel;
    
    public CassandraColumnFamilyInputFormat(CassandraDataImportJobModel importJobModel,
            boolean reparsableRecordSource) {
        super(importJobModel, reparsableRecordSource);
        this.importJobModel = importJobModel;
    }

    @Override
    protected void onConfigure(JobConf jobConf) {
        // anything special?
        // 
        //importJobModel.getConfiguration();
        // deduce the token space and host ring like CFIF
    }

    @Override
    public RecordParser<CassandraRowRecord> createRecordParser(Field[] arg0) throws IOException {
        return new MapBasedRecordParser<CassandraRowRecord>(arg0, new MapParser<CassandraRowRecord>() {

            @Override
            public void configureSchemaDetection(MapBasedRecordSchemaDetector<CassandraRowRecord> arg0, TextFieldAnalyzer arg1) {
                // dont need to do anythng?
            }

            @Override
            public Map<String, Object> parseRecordSource(CassandraRowRecord arg0) throws Exception {
                // field "origin" will be the column name
                // map.put(columnName, value)
                return null;
            }
        });
    }

    

    @Override
    public RecordSchemaDetector<CassandraRowRecord> createRecordSchemaDetector() throws IOException {
        return new NoDataRecordSchemaDetector<CassandraRowRecord>() {
            @Override
            public Field[] guessFields() {
                // loop through the config and pull the column names
                return new Field[] { };
            }
        };

    }

    @Override
    public RecordSourceReader<CassandraRowRecord> createRecordSourceReader(InputSplit arg0) throws IOException {
        // record soure reader implements readNext() which will pull the row off the slice for this split
        // TODO bring over ColumnFamilyRecordReader, change initialize to use Configuration
        return null;
    }

    @Override
    public long estimateRecordCount(InputSplit[] arg0) throws IOException {
        return 0;
    }

    @Override
    public Splitter getSplitter() throws IOException {
        // hand off connection information to noodle tokens and ring
        return new CassandraColumnFamilySplitter();
    }
    
    

}
