package com.riptano.datameer.importjob;

import datameer.dap.sdk.common.DasContext;
import datameer.dap.sdk.datastore.DataStoreModel;
import datameer.dap.sdk.datastore.DataStoreType;
import datameer.dap.sdk.entity.DataStore;

public class CassandraDataStoreType extends DataStoreType {

    public final static String ID = CassandraDataStoreType.class.getName();


    public CassandraDataStoreType() {
        super(new CassandraDataImportJobType());
    }

    @Override
    public DataStoreModel createModel(DasContext context, DataStore dataStore) {
        // do we pass props here? in in CDSM.initFrom()
        return new CassandraDataStoreModel();
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public String getName() {
        return "Cassandra Data Creator";
    }


    @Override
    public DataStoreModel createModel(DasContext arg0) {
        
        return null;
    }

}
