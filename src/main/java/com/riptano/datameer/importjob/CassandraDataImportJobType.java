package com.riptano.datameer.importjob;

import datameer.dap.sdk.entity.DataSourceConfiguration;
import datameer.dap.sdk.importjob.ImportJobModel;
import datameer.dap.sdk.importjob.ImportJobType;

public class CassandraDataImportJobType extends ImportJobType<CassandraRowRecord> {

    @Override
    public ImportJobModel<CassandraRowRecord> createModel(DataSourceConfiguration arg0) {
        return new CassandraDataImportJobModel(arg0);
    }

}
