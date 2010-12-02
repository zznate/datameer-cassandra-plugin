package com.riptano.datameer.importjob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import me.prettyprint.cassandra.service.CassandraHost;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.entity.DataSourceConfiguration;
import datameer.dap.sdk.function.FieldType;
import datameer.dap.sdk.importjob.AbstractImportFormat;
import datameer.dap.sdk.importjob.ImportFormat;
import datameer.dap.sdk.importjob.ImportJobModel;
import datameer.dap.sdk.importjob.MapBasedRecordParser;
import datameer.dap.sdk.importjob.MapBasedRecordSchemaDetector;
import datameer.dap.sdk.importjob.MapParser;
import datameer.dap.sdk.importjob.NoDataRecordSchemaDetector;
import datameer.dap.sdk.importjob.RecordParser;
import datameer.dap.sdk.importjob.RecordSchemaDetector;
import datameer.dap.sdk.importjob.RecordSourceReader;
import datameer.dap.sdk.importjob.TextFieldAnalyzer;
import datameer.dap.sdk.property.NonEmptyValidator;
import datameer.dap.sdk.property.NumberValidator;
import datameer.dap.sdk.property.PropertyDefinition;
import datameer.dap.sdk.property.PropertyGroupDefinition;
import datameer.dap.sdk.property.PropertyType;
import datameer.dap.sdk.property.WizardPageDefinition;

/**
 * Models job-specific information for the import job. In the Cassandra 
 * sense of things, this holds the Keyspace and ColumnFamily information
 * against which this job will run.
 * This also holds information about the nodes in the cluster and the token 
 * ring that will be needed by {@link CassandraColumnFamilyInputFormat}.
 * For Datameer, this holds the runtime properties that are needed for 
 * {@link CassandraDataImportJobType} to run. 
 * 
 * @author zznate <nate@riptano.com>
 */
public class CassandraDataImportJobModel extends ImportJobModel<CassandraRowRecord> {

    private static final String KEYSPACE = "cassandra.keyspace";
    private static final String COLUMN_FAMILY = "cassandra.columnFamily";
    private static final String COLUMNS = "cassandra.columns";    
    private static final String BATCH_SIZE = "cassandra.batchSize";
    private static final String SLICE_COUNT = "cassandra.sliceCount";
    
    private String keyspace;
    private String columnFamily;
    private List<String> columnNames;
    private int batchCount;
    private int sliceCount;
    private CassandraDataStoreModel dataStoreModel;
    private String[] hostUrls;
    private AtomicInteger current = new AtomicInteger();
    
    public CassandraDataImportJobModel(DataSourceConfiguration conf) {
        super(conf);
        dataStoreModel = new CassandraDataStoreModel(conf.getDataStore());
        keyspace = conf.getStringProperty(KEYSPACE, null);
        columnFamily = conf.getStringProperty(COLUMN_FAMILY, null);
        // if columnNames is empty, we'll take them all
        columnNames = new ArrayList<String>();
        if ( conf.getStringProperty(COLUMNS, null) != null ) {
            for (String col : conf.getStringProperty(COLUMNS, null).split(",")) {
                columnNames.add(col);
            }
        }                
        CassandraHost[] cassandraHosts = dataStoreModel.getCassandraHostConfigurator().buildCassandraHosts();
        hostUrls = new String[cassandraHosts.length];
        for (int i = 0; i < cassandraHosts.length; i++) {
            hostUrls[i] = cassandraHosts[i].getUrl();
        }
            
        
        batchCount = conf.getIntProperty(BATCH_SIZE, 10000);
        sliceCount = conf.getIntProperty(SLICE_COUNT, 100);
    }    
    
    public String getKeyspace() {
        return keyspace;
    }

    public String getColumnFamily() {
        return columnFamily;
    }
    
    public List<String> getColumnNames() {
        return columnNames;
    }
    
    public int getBatchCount() {
        return batchCount;
    }

    public int getSliceCount() {
        return sliceCount;
    }   
    
    public boolean getHasColumns() {
        return !columnNames.isEmpty();
    }
        
    public CassandraDataStoreModel getCassandraDataStoreModel() {
        return dataStoreModel;
    }
    
    public String getNextHost() {
        int i = current.incrementAndGet();
        if (i >= hostUrls.length ) {            
            current.compareAndSet(i, 0);
            i = 0;
        }
        return hostUrls[i];
    }
    
    public SlicePredicate getSlicePredicate() {
        SlicePredicate sp = new SlicePredicate();
        if ( columnNames != null && columnNames.size() > 0 ) {
            for (String colName : columnNames) {
                try {
                    sp.addToColumn_names(colName.getBytes("UTF-8"));
                } catch (Exception e) { }            
            }
        } else {
            sp.setSlice_range(new SliceRange(new byte[]{}, new byte[]{}, false, 2));
        }
        return sp;
    }

    @Override
    public ImportFormat<CassandraRowRecord> createImportFormat() {
        return new CassandraColumnFamilyInputFormat(this, true);
    }

    @Override
    public void addPropertyValuesThatTriggerAFieldReset(List<Object> propertyValues) {
    }

    @Override
    public WizardPageDefinition createDetailsWizardPage() {
        WizardPageDefinition page = new WizardPageDefinition("Details");
        PropertyGroupDefinition group = page.addGroup("Cassandra Configuration Data");
        PropertyDefinition propertyDefinition = new PropertyDefinition(KEYSPACE, "The Keyspace to use. Similar to a 'database' in SQL", PropertyType.STRING);
        propertyDefinition.setRequired(true);
        propertyDefinition.setValidators(new NonEmptyValidator());
        group.addPropertyDefinition(propertyDefinition);

        propertyDefinition = new PropertyDefinition(COLUMN_FAMILY, "The ColumnFamily which holds the data. Similar to 'table' in SQL", PropertyType.STRING);
        propertyDefinition.setRequired(true);
        propertyDefinition.setValidators(new NonEmptyValidator());
        group.addPropertyDefinition(propertyDefinition);

        propertyDefinition = new PropertyDefinition(COLUMNS, "The Columns which will be imported (comma separated)", PropertyType.STRING);
        propertyDefinition.setRequired(false);
        propertyDefinition.setValidators(new NonEmptyValidator());
        group.addPropertyDefinition(propertyDefinition);     
        
        propertyDefinition = new PropertyDefinition(BATCH_SIZE, "The number of rows to read off for a split. The default is 10000", PropertyType.STRING);
        propertyDefinition.setRequired(false);
        propertyDefinition.setValidators(new NonEmptyValidator());
        group.addPropertyDefinition(propertyDefinition); 
        
        return page;
    }

    @Override
    public WizardPageDefinition createImportJobFilterPage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSupportImportJobFilterPage() {
        return false;
    }

    @Override
    public boolean canAutoMergeNewFields() {
        return true;
    }

    @Override
    public void addPropertyValuesThatTriggerAFilterReset(List<Object> arg0) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void resetFilters() {
        // TODO Auto-generated method stub
        
    }
}

