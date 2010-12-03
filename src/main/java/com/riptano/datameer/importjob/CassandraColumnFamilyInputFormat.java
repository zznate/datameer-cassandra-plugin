package com.riptano.datameer.importjob;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.importjob.AbstractImportFormat;
import datameer.dap.sdk.importjob.RecordParser;
import datameer.dap.sdk.importjob.RecordSchemaDetector;
import datameer.dap.sdk.importjob.RecordSourceReader;


public class CassandraColumnFamilyInputFormat extends AbstractImportFormat<CassandraRowRecord> {

    private static Logger log = Logger.getLogger(CassandraColumnFamilyInputFormat.class);
    
    private CassandraDataImportJobModel importJobModel;
    private Configuration configuration;
    
    public CassandraColumnFamilyInputFormat(CassandraDataImportJobModel importJobModel,
            boolean reparsableRecordSource) {
        super(importJobModel, reparsableRecordSource);
        this.importJobModel = importJobModel;
        
    }

    @Override
    protected void onConfigure(JobConf jobConf) {
        this.configuration = jobConf;
    }
    
    
    @Override
    public RecordParser<CassandraRowRecord> createRecordParser(Field[] arg0) throws IOException {
       return new CassandraRecordParser(arg0, importJobModel);
    }

    

    @Override
    public RecordSchemaDetector<CassandraRowRecord> createRecordSchemaDetector() throws IOException {
        return new CassandraRowRecordSchemaDector(importJobModel);
    }
    

    @Override
    public RecordSourceReader<CassandraRowRecord> createRecordSourceReader(InputSplit arg0) throws IOException {
        return new CassandraColumnFamilyRecordSourceReader(importJobModel, (CassandraColumnFamilySplit)arg0, configuration);
    }

    @Override
    public long estimateRecordCount(InputSplit[] arg0) throws IOException {
        if ( log.isDebugEnabled() ) {
            log.debug("in CCFIF.estimateRecordCount with IS[] " + Arrays.asList(arg0));
        }
        return 0;
    }

    @Override
    public CassandraColumnFamilySplitter getSplitter() throws IOException {
        // hand off connection information to noodle tokens and ring
        return new CassandraColumnFamilySplitter(importJobModel);
    }
    
    

}
