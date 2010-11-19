package com.riptano.datameer.importjob;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
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
import datameer.dap.sdk.importjob.Splitter;
import datameer.dap.sdk.importjob.TextFieldAnalyzer;


public class CassandraColumnFamilyInputFormat extends AbstractImportFormat<CassandraRowRecord> {

    private CassandraDataImportJobModel importJobModel;
    private Configuration configuration;
    
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
        //jobConf.get
        this.configuration = jobConf;
    }
    
    

    @Override
    public RecordParser<CassandraRowRecord> createRecordParser(Field[] arg0) throws IOException {
        return new MapBasedRecordParser<CassandraRowRecord>(arg0, new MapParser<CassandraRowRecord>() {

            @Override
            public void configureSchemaDetection(MapBasedRecordSchemaDetector<CassandraRowRecord> arg0, TextFieldAnalyzer arg1) {
                // dont need to do anythng?
                System.out.print("in CCFIF.configureSchemaDetection w. CRR: " + arg0 + " and TFA: " +arg1);
            }

            @Override
            public Map<String, Object> parseRecordSource(CassandraRowRecord arg0) throws Exception {

                Map<String, Object> source = new HashMap<String, Object>();
                source.put("key", arg0.getKey());
                for (Map.Entry<byte[], IColumn> column : arg0.getRows().entrySet()) {
                    source.put(new String(column.getKey(), "utf-8"), column.getValue());
                }
                return source;
            }
        });
    }

    

    @Override
    public RecordSchemaDetector<CassandraRowRecord> createRecordSchemaDetector() throws IOException {
        return new NoDataRecordSchemaDetector<CassandraRowRecord>() {
            @Override
            public Field[] detectFields() {
                // loop through the config and pull the column names
                System.out.print("in CCFIF.createRecordSchemaDetector with...");
                return new Field[] { };
            }
        };

    }

    @Override
    public RecordSourceReader<CassandraRowRecord> createRecordSourceReader(InputSplit arg0) throws IOException {
        // record soure reader implements readNext() which will pull the row off the slice for this split
        // TODO bring over ColumnFamilyRecordReader, change initialize to use Configuration
        
        return new CassandraColumnFamilyRecordSourceReader(importJobModel, (CassandraColumnFamilySplit)arg0, configuration);
    }

    @Override
    public long estimateRecordCount(InputSplit[] arg0) throws IOException {
        System.out.print("in CCFIF.estimateRecordCount with IS[] " + Arrays.asList(arg0));
        return 0;
    }

    @Override
    public CassandraColumnFamilySplitter getSplitter() throws IOException {
        // hand off connection information to noodle tokens and ring
        return new CassandraColumnFamilySplitter(importJobModel);
    }
    
    

}
