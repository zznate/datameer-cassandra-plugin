package com.riptano.datameer.importjob;

import datameer.dap.sdk.common.Field;
import datameer.dap.sdk.function.FieldType;
import datameer.dap.sdk.importjob.NoDataRecordSchemaDetector;

public class CassandraRowRecordSchemaDector extends NoDataRecordSchemaDetector<CassandraRowRecord> {

    
    
    @Override
    public Field[] detectFields() {
        return new Field[] { new Field("name", "01", FieldType.STRING, true)};
    }

    
}

