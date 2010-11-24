package com.riptano.datameer.importjob;

import java.util.List;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.function.FieldType;
import datameer.dap.sdk.importjob.NoDataRecordSchemaDetector;

public class CassandraRowRecordSchemaDector extends NoDataRecordSchemaDetector<CassandraRowRecord> {

    private CassandraDataImportJobModel dataImportJobModel;
    
    public CassandraRowRecordSchemaDector(CassandraDataImportJobModel dataImportJobModel) {
        this.dataImportJobModel = dataImportJobModel;
    }
    
    @Override
    public Field[] detectFields() {
        List<String> columns = dataImportJobModel.getColumnNames();
        Field[] fields = null;
        if ( columns != null && columns.size() > 0) {
            fields = new Field[columns.size()];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = new Field(columns.get(i), columns.get(i), FieldType.STRING, true);                                                            
            }
        }
        return fields; 
    }

    
}

