package com.manywho.services.sql.managers;

import com.manywho.sdk.api.run.elements.type.ListFilter;
import com.manywho.sdk.api.run.elements.type.MObject;
import com.manywho.sdk.api.run.elements.type.ObjectDataType;
import com.manywho.services.sql.ServiceConfiguration;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.services.DataService;
import com.manywho.services.sql.services.PrimaryKeyService;
import com.manywho.services.sql.services.QueryStrService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.sql2o.Connection;
import org.sql2o.Sql2o;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;

public class DataManager {

    private DataService dataService;
    private QueryStrService queryStrService;
    private PrimaryKeyService primaryKeyService;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataService.class);

    @Inject
    public DataManager(DataService dataService, QueryStrService queryStrService, PrimaryKeyService primaryKeyService){
        this.dataService = dataService;
        this.queryStrService = queryStrService;
        this.primaryKeyService = primaryKeyService;
    }

    public List<MObject> load(Connection connection, ServiceConfiguration configuration, TableMetadata tableMetadata,
                              HashMap<String, String> id) throws Exception {

        return dataService.fetchByPrimaryKey(tableMetadata, connection, id, configuration);
    }

    public List<MObject> loadBySearch(Sql2o sql2o,ServiceConfiguration configuration, TableMetadata tableMetadata,
                                      ObjectDataType objectDataType, ListFilter filters) throws Exception {

        String queryString = "";

        try {
            queryString = queryStrService.getSqlFromFilter(configuration, objectDataType, filters, tableMetadata);

            return dataService.fetchBySearch(tableMetadata, sql2o, queryString);
        } catch (Exception ex) {
            LOGGER.error("query: " + queryString, ex);
            throw ex;
        }
    }

    /**
     * runs the database update in the service depending on the changes in the MObject given, returns the MObject with
     * an updated primary key
     *
     * @param connection the Sql2o connection object, can be either a standard object or a transaction
     * @param configuration the database configuration
     * @param tableMetadata the table metadata
     * @param mObject the mobject with the changes made in it's properties and a primary key set in the external ID
     * @return the mobject with an updated primary key, if the update worked then what was asked for is set.
     * @exception Exception if it can't deserialise the primary key or if the update fails
     */
    public MObject update(Connection connection, ServiceConfiguration configuration, TableMetadata tableMetadata, MObject mObject) throws Exception {
        HashMap<String, String> primaryKeyHashMap = primaryKeyService.deserializePrimaryKey(mObject.getExternalId());
        mObject = dataService.update(mObject, connection, tableMetadata, primaryKeyHashMap, configuration);
        mObject.setExternalId(primaryKeyService.serializePrimaryKey(primaryKeyService.updateFromObject(primaryKeyHashMap,mObject)));
        return mObject;
    }

    public MObject create(Connection connection, ServiceConfiguration configuration, TableMetadata tableMetadata, MObject mObject) throws Exception {
        return dataService.insert(mObject, connection, tableMetadata, configuration);
    }

    public void delete(Sql2o sql2o, ServiceConfiguration configuration, TableMetadata tableMetadata,
                       HashMap<String, String> id) throws Exception {
        dataService.deleteByPrimaryKey(tableMetadata, sql2o, id, configuration);
    }
}
