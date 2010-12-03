package com.riptano.datameer.importjob;

import datameer.dap.sdk.common.DasContext;
import datameer.dap.sdk.datastore.DataStoreModel;
import datameer.dap.sdk.datastore.DataStoreType;
import datameer.dap.sdk.entity.DataStore;
import datameer.dap.sdk.property.NonEmptyValidator;
import datameer.dap.sdk.property.PropertyDefinition;
import datameer.dap.sdk.property.PropertyGroupDefinition;
import datameer.dap.sdk.property.PropertyType;
import datameer.dap.sdk.property.WizardPageDefinition;

public class CassandraDataStoreType extends DataStoreType {

    public static final String ID_STR = "Apache Cassandra 0.6.x";

    public CassandraDataStoreType() {
        super(new CassandraDataImportJobType());
    }

    @Override
    public DataStoreModel createModel(DasContext context, DataStore dataStore) {
        return new CassandraDataStoreModel(dataStore);
    }

    @Override
    public String getId() {
        return ID_STR;
    }

    @Override
    public String getName() {
        return ID_STR;
    }


    @Override
    public WizardPageDefinition createDetailsWizardPage() {        

        WizardPageDefinition page = new WizardPageDefinition("Apache Cassandra Connection Details");
        PropertyGroupDefinition group = page.addGroup("Apache Cassandra Connection Configuration Data");
        PropertyDefinition propertyDefinition = new PropertyDefinition(CassandraDataStoreModel.HOSTS, 
                "Hosts", 
                PropertyType.STRING);
        propertyDefinition.setHelpText("Hosts should be specified in comma-delimited hosts/port combinations. For example: 'localhost:9160' or 'cass1:9160,cass2:9160,cass3:9160'");
        propertyDefinition.setRequired(true);
        propertyDefinition.setValidators(new NonEmptyValidator());
        group.addPropertyDefinition(propertyDefinition);
        
        propertyDefinition = new PropertyDefinition(CassandraDataStoreModel.USE_FRAMED, 
                "Use TFramedTransport", PropertyType.BOOLEAN);
        propertyDefinition.setRequired(false);
        propertyDefinition.setHelpText("Check if you have your cluster configured to use TFramedTransport.");
        group.addPropertyDefinition(propertyDefinition);
        
        propertyDefinition = new PropertyDefinition(CassandraDataStoreModel.PRESERVE_LOCALITY, 
                "Preserve Locality", 
                PropertyType.BOOLEAN);
        propertyDefinition.setRequired(false);
        propertyDefinition.setHelpText("Check if Thrift and Gossip share the same interface (allows usage of the more efficient describe_splits method). Leave this unchecked if you are not sure.");
        group.addPropertyDefinition(propertyDefinition);

        return page;
    }

}
