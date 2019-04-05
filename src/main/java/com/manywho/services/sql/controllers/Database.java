package com.manywho.services.sql.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.manywho.sdk.api.run.elements.type.ListFilter;
import com.manywho.sdk.api.run.elements.type.MObject;
import com.manywho.sdk.api.run.elements.type.ObjectDataType;
import com.manywho.sdk.services.database.RawDatabase;
import com.manywho.services.sql.ServiceConfiguration;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.exceptions.RecordNotFoundException;
import com.manywho.services.sql.managers.ConnectionManager;
import com.manywho.services.sql.managers.DataManager;
import com.manywho.services.sql.managers.MetadataManager;
import com.manywho.services.sql.services.AliasService;
import com.manywho.services.sql.services.PrimaryKeyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;

public class Database implements RawDatabase<ServiceConfiguration> {
    private DataManager dataManager;
    private PrimaryKeyService primaryKeyService;
    private MetadataManager metadataManager;
    private AliasService aliasService;
    private ObjectMapper objectMapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);


    @Inject
    public Database(DataManager dataManager, PrimaryKeyService primaryKeyService, MetadataManager metadataManager,
                    AliasService aliasService) {
        this.dataManager = dataManager;
        this.primaryKeyService = primaryKeyService;
        this.metadataManager = metadataManager;
        this.aliasService = aliasService;
        objectMapper = new ObjectMapper();
    }

    @Override
    public MObject create(ServiceConfiguration configuration, MObject object) {

        Sql2o sql2o = ConnectionManager.getSql2Object(configuration);

        try (Connection connection = sql2o.open()) {
            TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration, object.getDeveloperName());

            MObject objectWithColumnName = this.aliasService.getMObjectWithoutAliases(object, tableMetadata);
            MObject objectInserted = dataManager.create(connection, configuration, tableMetadata, objectWithColumnName);
            HashMap<String, String> primaryKey = primaryKeyService.deserializePrimaryKey(objectInserted.getExternalId());
            List<MObject> mObjectList = this.dataManager.load(connection, configuration, tableMetadata, primaryKey);

            if (mObjectList.size() > 0) {
                return this.aliasService.getMObjectWithAliases(mObjectList.get(0), tableMetadata);
            }

        } catch (Exception e) {
            try {
                LOGGER.error("create MObject: " + objectMapper.writeValueAsString(object), e);
            } catch (Exception ignored) {}

            throw new RuntimeException("problem creating object" + e.getMessage());
        }

        throw new RuntimeException("Error creating object");
    }

    @Override
    public List<MObject> create(ServiceConfiguration configuration, List<MObject> objects) {
        List<MObject> objectsCreated = Lists.newArrayList();
        objects.forEach((object) -> {
            objectsCreated.add(create(configuration, object));
        });
        return objectsCreated;
    }

    @Override
    public void delete(ServiceConfiguration configuration, MObject object) {
        Sql2o sql2o = ConnectionManager.getSql2Object(configuration);
        try (Connection connection = sql2o.open()) {

            TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration, object.getDeveloperName());
            MObject objectWithOriginalNames = this.aliasService.getMObjectWithoutAliases(object, tableMetadata);

            this.dataManager.delete(sql2o, configuration, tableMetadata,
                    primaryKeyService.deserializePrimaryKey(objectWithOriginalNames.getExternalId()));

        } catch (Exception e) {
            try {
                LOGGER.error("delete MObject: " + objectMapper.writeValueAsString(object), e);
            } catch (Exception ignored) {}

            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(ServiceConfiguration configuration, List<MObject> objects) {
        objects.forEach((object) -> {
            delete(configuration, object);
        });
        return;
    }

    @Override
    public MObject find(ServiceConfiguration configuration, ObjectDataType objectDataType, String id) {
        Sql2o sql2o = ConnectionManager.getSql2Object(configuration);

        try (Connection connection = sql2o.open()) {
            TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration,
                    objectDataType.getDeveloperName());

            List<MObject> mObjectList = this.dataManager.load(connection, configuration, tableMetadata,
                    aliasService.getOriginalKeys(primaryKeyService.deserializePrimaryKey(id), tableMetadata));

            if (mObjectList.size() > 0) {
                return this.aliasService.getMObjectWithAliases(mObjectList.get(0), tableMetadata);
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        throw new RecordNotFoundException();
    }

    @Override
    public List<MObject> findAll(ServiceConfiguration configuration, ObjectDataType objectDataType, ListFilter filter) {
        Sql2o sql2o = ConnectionManager.getSql2Object(configuration);

        try (Connection connection = sql2o.open()) {
            TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration, objectDataType.getDeveloperName());
            List<MObject> mObjects = this.dataManager.loadBySearch(sql2o, configuration, tableMetadata, objectDataType, filter);
            return this.aliasService.getMObjectsWithAlias(mObjects, tableMetadata);


        } catch (Exception e) {
            try {
                LOGGER.error("findAll filter: " + objectMapper.writeValueAsString(filter), e);
            } catch (Exception ignored) {}

            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public MObject update(ServiceConfiguration configuration, MObject object) throws  RecordNotFoundException{
        Sql2o sql2o = ConnectionManager.getSql2Object(configuration);
        try (Connection connection = sql2o.open()) {
            return doUpdate(connection, configuration, object);
        } catch (RecordNotFoundException e) {
            throw e;
        } catch (Exception e) {
            try {
                LOGGER.error("update MObject: " + objectMapper.writeValueAsString(object), e);
            } catch (Exception ignored) {}
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<MObject> update(ServiceConfiguration configuration, List<MObject> objects) throws RecordNotFoundException {
        Sql2o sql2o = ConnectionManager.getSql2Object(configuration);

        List<MObject> objectsUpdated = Lists.newArrayList();
        List<MObject> updatedObjects = Lists.newArrayList();

        try (Connection connection = sql2o.beginTransaction()) {

            objects.forEach((object) -> {
                try {
                    TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration,
                            object.getDeveloperName());

                    object = aliasService.getMObjectWithoutAliases(object, tableMetadata);
                    objectsUpdated.add(this.dataManager.update(connection, configuration, tableMetadata, object));
                } catch (Exception e) {
                    throw new RecordNotFoundException();
                }
            });

            connection.commit();

            objectsUpdated.forEach((object) -> {
                try {
                    TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration,
                            object.getDeveloperName());

                    object = aliasService.getMObjectWithoutAliases(object, tableMetadata);
                    List<MObject> mObjectList = this.dataManager.load(connection, configuration, tableMetadata,
                            primaryKeyService.deserializePrimaryKey(object.getExternalId()));
                    if (mObjectList.size() > 0) {
                        MObject mObject = mObjectList.get(0);
                        updatedObjects.add(this.aliasService.getMObjectWithAliases(mObject, tableMetadata));
                    } else {
                        throw new RecordNotFoundException();
                    }
                } catch (Exception e) {
                    throw new RecordNotFoundException();
                }
            });
        }
        return updatedObjects;
    }

    /**
     * Does the actual update of the object; will get the table metadata, gets the column names from the alias service,
     * runs the update, then returns the object with the alias's if they're available, ::TODO:: should probably check if
     * the incoming object was using aliases in the first place and only return the aliases if they were set, however,
     * this should probably probably be the remit of the alias service... (infact maybe this is where we should maybe
     * start to be clever and store the original object and then extract any aliases into a separate member that
     * optionally replaces the developername on serialisation?)
     *
     * @param connection the Sql2o database connection either a standard connection or a transaction
     * @param configuration the configuration object for the database
     * @param object the object that has a primary key set to update (with the updates to do)
     * @return the updated MObject
     * @exception Exception if can't find metadata or if update fails itself
     */
    private MObject doUpdate (Connection connection, ServiceConfiguration configuration, MObject object) throws Exception {
        // get metadata
        TableMetadata tableMetadata = metadataManager.getMetadataTable(connection, configuration, object.getDeveloperName());
        // convert any (external) aliases to internal column names
        object = aliasService.getMObjectWithoutAliases(object, tableMetadata);
        // run the actual update
        object = this.dataManager.update(connection, configuration, tableMetadata, object);
        // return the object with the internal column names updated to any external aliases if they exist
        return this.aliasService.getMObjectWithAliases(object, tableMetadata);
    }
}