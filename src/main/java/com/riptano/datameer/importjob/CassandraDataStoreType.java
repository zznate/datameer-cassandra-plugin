package com.riptano.datameer.importjob;

import datameer.dap.sdk.common.DasContext;
import datameer.dap.sdk.datastore.DataStoreModel;
import datameer.dap.sdk.datastore.DataStoreType;
import datameer.dap.sdk.entity.DataStore;
import datameer.dap.sdk.importjob.ImportJobType;
import datameer.dap.sdk.property.NonEmptyValidator;
import datameer.dap.sdk.property.PropertyDefinition;
import datameer.dap.sdk.property.PropertyGroupDefinition;
import datameer.dap.sdk.property.PropertyType;
import datameer.dap.sdk.property.WizardPageDefinition;

public class CassandraDataStoreType extends DataStoreType {

    public final static String ID = CassandraDataStoreType.class.getName();    

    public CassandraDataStoreType() {
        super(new CassandraDataImportJobType());
    }

    @Override
    public DataStoreModel createModel(DasContext context, DataStore dataStore) {
        // do we pass props here? in in CDSM.initFrom()
        return new CassandraDataStoreModel(dataStore);
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
    public WizardPageDefinition createDetailsWizardPage() {        

        WizardPageDefinition page = new WizardPageDefinition("Apache Cassandra Connection Details");
        PropertyGroupDefinition group = page.addGroup("Apache Cassandra Connection Configuration Data");
        PropertyDefinition propertyDefinition = new PropertyDefinition(CassandraDataStoreModel.HOSTS, 
                "One or more comma-delimited hosts/port combinations. For example: 'localhost:9160' or 'cass1:9160,cass2:9160,cass3:9160'", 
                PropertyType.STRING);
        propertyDefinition.setRequired(true);
        propertyDefinition.setValidators(new NonEmptyValidator());
        group.addPropertyDefinition(propertyDefinition);
        
        propertyDefinition = new PropertyDefinition(CassandraDataStoreModel.USE_FRAMED, 
                "True if configured for TFramedTransport", PropertyType.BOOLEAN);
        propertyDefinition.setRequired(false);
        group.addPropertyDefinition(propertyDefinition);
        
        propertyDefinition = new PropertyDefinition(CassandraDataStoreModel.PRESERVE_LOCALITY, 
                "True if Thrift and Gossip share the same interface (allows usage of the more efficient describe_splits method).", 
                PropertyType.BOOLEAN);
        propertyDefinition.setRequired(false);
        group.addPropertyDefinition(propertyDefinition);

        return page;
    }

}
