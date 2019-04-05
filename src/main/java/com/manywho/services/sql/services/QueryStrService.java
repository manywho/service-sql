package com.manywho.services.sql.services;

import com.healthmarketscience.sqlbuilder.*;
import com.manywho.sdk.api.run.elements.type.ListFilter;
import com.manywho.sdk.api.run.elements.type.MObject;
import com.manywho.sdk.api.run.elements.type.ObjectDataType;
import com.manywho.sdk.api.run.elements.type.Property;
import com.manywho.services.sql.ServiceConfiguration;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.services.filter.QueryFilterConditions;
import com.manywho.services.sql.utilities.ScapeForTablesUtil;

import javax.inject.Inject;
import java.util.Set;

public class QueryStrService {
    private QueryFilterConditions queryFilterConditions;
    private ScapeForTablesUtil scapeForTablesUtil;
    private AliasService aliasService;

    @Inject
    public QueryStrService(QueryFilterConditions queryFilterConditions, ScapeForTablesUtil scapeForTablesUtil,
                           AliasService aliasService) {
        this.queryFilterConditions = queryFilterConditions;
        this.scapeForTablesUtil = scapeForTablesUtil;
        this.aliasService = aliasService;
    }

    public String createQueryWithParametersForSelectByPrimaryKey(TableMetadata tableMetadata, Set<String> primaryKeyNames, ServiceConfiguration configuration) {

        SelectQuery selectQuery = new SelectQuery().addAllColumns()
                .addCustomFromTable(scapeForTablesUtil.scapeTableName(configuration.getDatabaseType(),tableMetadata.getSchemaName(), tableMetadata.getTableName()));

        for (String key: primaryKeyNames) {
            selectQuery.addCondition(BinaryCondition.equalTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(configuration.getDatabaseType(),key)), new CustomSql(":" + key)));
        }

        return selectQuery.validate().toString();
    }

    public String createQueryWithParametersForDeleteByPrimaryKey(TableMetadata tableMetadata, Set<String> primaryKeyNames, ServiceConfiguration configuration) {

        DeleteQuery selectQuery = new DeleteQuery(scapeForTablesUtil.scapeTableName(configuration.getDatabaseType(), tableMetadata.getSchemaName(), tableMetadata.getTableName()));

        for (String key: primaryKeyNames) {
            selectQuery.addCondition(BinaryCondition.equalTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(configuration.getDatabaseType(),key)), new CustomSql(":" + key)));
        }

        return selectQuery.validate().toString();
    }

    public String createQueryWithParametersForUpdate(MObject mObject, TableMetadata tableMetadata, Set<String> primaryKeyNames, ServiceConfiguration configuration){

        UpdateQuery updateQuery = new UpdateQuery(
                scapeForTablesUtil.scapeTableName(configuration.getDatabaseType(), tableMetadata.getSchemaName(), tableMetadata.getTableName()));

        for(Property p : mObject.getProperties()) {
            // definitely don't update auto increments! :) ::TODO:: there is scope where we could update the auto
            // increment to one more than the max of the column after such an update but future work maybe
           if(!tableMetadata.isColumnAutoincrement(p.getDeveloperName())) {
               updateQuery.addCustomSetClause(new CustomSql(ScapeForTablesUtil.scapeCollumnName(configuration.getDatabaseType(), p.getDeveloperName())), new CustomSql(":" + p.getDeveloperName()));
           }
        }

        for (String key: primaryKeyNames) {
            updateQuery.addCondition(
                    BinaryCondition.equalTo(
                            new CustomSql(
                                    ScapeForTablesUtil.scapeCollumnName(
                                            configuration.getDatabaseType(),
                                            key
                                    )
                            ),
                            // here I'm adding the primary key in with a prefix as we're making them named parameters
                            // and not numbered so that if we want to update the primary key when we select in a
                            // condition by the primary key we don't accidentally add the wrong value to the primary key
                            // ::TODO:: this should probably be a class variable somewhere or like a constant
                            // ::TODO:: it may be worth doing the same with the others so that we don't trip over--...
                            // ourselves if someone names a column "keyid" and also has a primary key of "id"...
                            new CustomSql(":key" + key)
                    )
            );
        }

        return updateQuery.validate().toString();
    }

    public String createQueryWithParametersForInsert(MObject mObject, TableMetadata tableMetadata, ServiceConfiguration configuration) {
        InsertQuery insertQuery = new InsertQuery(
                scapeForTablesUtil.scapeTableName(configuration.getDatabaseType(), tableMetadata.getSchemaName(), tableMetadata.getTableName()));

        for(Property p : mObject.getProperties()) {
            if (!tableMetadata.isColumnAutoincrement(p.getDeveloperName())) {
                insertQuery.addCustomColumn(new CustomSql(ScapeForTablesUtil.scapeCollumnName(configuration.getDatabaseType(), p.getDeveloperName())), new CustomSql(":" + p.getDeveloperName()));
            }
        }

        return  insertQuery.validate().toString();
    }

    public String getSqlFromFilter(ServiceConfiguration configuration, ObjectDataType objectDataType, ListFilter filter, TableMetadata tableMetadata) {

        SelectQuery selectQuery = new SelectQuery().addAllColumns()
                .addCustomFromTable(scapeForTablesUtil.scapeTableName(configuration.getDatabaseType(),
                        configuration.getDatabaseSchema(), objectDataType.getDeveloperName()));

        aliasService.setFiltersOriginalNames(tableMetadata, filter);
        objectDataType.setProperties(aliasService.setPropertiesOriginalName(tableMetadata, objectDataType.getProperties()));

        queryFilterConditions.addSearch(selectQuery, filter.getSearch(), objectDataType.getProperties(), tableMetadata.getColumnsDatabaseType(), configuration.getDatabaseType());
        queryFilterConditions.addWhere(selectQuery, filter.getWhere(), filter.getComparisonType(), configuration.getDatabaseType(), tableMetadata);
        queryFilterConditions.addOffset(selectQuery, configuration.getDatabaseType(), filter.getOffset(), filter.getLimit());
        queryFilterConditions.addOrderBy(selectQuery, filter.getOrderByPropertyDeveloperName(),
                filter.getOrderByDirectionType(), tableMetadata, configuration.getDatabaseType());

        return selectQuery.validate().toString();
    }
}
