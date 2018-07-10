package com.manywho.services.sql.services.filter;

import com.google.common.base.Strings;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.custom.mysql.MysLimitClause;
import com.healthmarketscience.sqlbuilder.custom.postgresql.PgLimitClause;
import com.healthmarketscience.sqlbuilder.custom.postgresql.PgOffsetClause;
import com.manywho.sdk.api.ComparisonType;
import com.manywho.sdk.api.run.elements.type.ListFilter;
import com.manywho.sdk.api.run.elements.type.ListFilterWhere;
import com.manywho.sdk.api.run.elements.type.ObjectDataTypeProperty;
import com.manywho.services.sql.entities.DatabaseType;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.utilities.ScapeForTablesUtil;

import java.sql.JDBCType;
import java.util.*;
import java.util.stream.Collectors;

public class QueryFilterConditions {
    public void addSearch(SelectQuery selectQuery, String search, List<ObjectDataTypeProperty> listProperties, HashMap<String, String> columns, DatabaseType databaseType) {
        if (Strings.isNullOrEmpty(search)) {
            return;
        }

        // Find all the string-ish columns, so we can compare the search criteria against them
        Map<String, String> stringishColumns = columns.entrySet().stream()
                .filter(column -> column.getValue().equals(JDBCType.VARCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.NVARCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.LONGVARCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.LONGNVARCHAR.getName()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));

        String searchTerm = "%" + search + "%";

        ComboCondition searchCondition = new ComboCondition(ComboCondition.Op.OR);

        for (ObjectDataTypeProperty property : listProperties) {
            if (stringishColumns.containsKey(property.getDeveloperName())) {
                searchCondition.addCondition(BinaryCondition.like(
                        new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, property.getDeveloperName())),
                        searchTerm
                ));
            }
        }

        selectQuery.addCondition(searchCondition);
    }

    public void addWhere(SelectQuery selectQuery, List <ListFilterWhere> whereList, ComparisonType comparisonType, DatabaseType databaseType) {
        ArrayList<Condition> conditions = new ArrayList<>();
        if(whereList == null) return;

        String comparisonTypeLocal = "";

        for (ListFilterWhere filterWhere: whereList) {
            conditions.add(getConditionFromFilterElement(filterWhere, databaseType));
        }

        if(comparisonType!=null) {
            comparisonTypeLocal = comparisonType.toString();
        }

        if ("OR".equals(comparisonTypeLocal)) {
            selectQuery.addCondition(ComboCondition.or(conditions.toArray()));
        }else if ("AND".equals(comparisonTypeLocal)) {
            selectQuery.addCondition(ComboCondition.and(conditions.toArray()));
        }
    }

    private BinaryCondition getConditionFromFilterElement(ListFilterWhere filterWhere, DatabaseType databaseType) {

        switch (filterWhere.getCriteriaType()) {
            case Equal:
               return BinaryCondition.equalTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue());
            case NotEqual:
               return BinaryCondition.notEqualTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue());
            case GreaterThan:
               return BinaryCondition.greaterThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue(), false);
            case GreaterThanOrEqual:
               return BinaryCondition.greaterThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue(), true);
            case LessThan:
               return BinaryCondition.lessThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue(), false);
            case LessThanOrEqual:
               return BinaryCondition.lessThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue(), true);
            case Contains:
               return BinaryCondition.like(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), "%" + filterWhere.getContentValue() + "%");
            case StartsWith:
               return BinaryCondition.like(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), filterWhere.getContentValue() + "%");
            case EndsWith:
               return BinaryCondition.like(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), "%" + filterWhere.getContentValue());
            case IsEmpty:
               return BinaryCondition.equalTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), BinaryCondition.EMPTY);
            default:
                break;
        }
        return null;
    }

    public void addOffset(SelectQuery selectQuery, DatabaseType databaseType, Integer offset, Integer limit) {

        if(limit <= 0 || limit > 1000) {
            limit = 1000;
        }
        switch (databaseType) {
            case Postgresql:
                    selectQuery.addCustomization(new PgOffsetClause(offset));
                    selectQuery.addCustomization(new PgLimitClause(limit));

                break;
            case Mysql:
                    selectQuery.addCustomization(new MysLimitClause(offset, limit));

                break;
            case Sqlserver:
                    selectQuery.setOffset(offset);
                    //selectQuery.addCustomization(new MssTopClause(limit));
                    selectQuery.setFetchNext(limit);

                break;
            // add oracle or any other jdbc supported database
            default:
                throw new RuntimeException("database type not supported");
        }
    }

    /**
     * It will order by orderByPropertyName, if this field is empty will order by the primary Keys
     *
     * @param selectQuery
     * @param orderByPropertyName
     * @param direction
     * @param tableMetadata
     * @param databaseType
     */
    public void addOrderBy(SelectQuery selectQuery, String orderByPropertyName, ListFilter.OrderByDirectionType direction, TableMetadata tableMetadata, DatabaseType databaseType) {
        OrderObject.Dir typeDirection = OrderObject.Dir.ASCENDING;

        if (Strings.isNullOrEmpty(orderByPropertyName) == true) {

            if (databaseType.equals(DatabaseType.Sqlserver)) {
                // When using Sql Server database ROW NUMBER requires an order by syntactically, this line fix the syntax without force an order by
                selectQuery.addCustomOrdering(new CustomSql("(SELECT NULL)"), typeDirection);
            }

            return;
        }

        if (direction != null && direction.equals(ListFilter.OrderByDirectionType.Descending)) {
            typeDirection = OrderObject.Dir.DESCENDING;
        }

        selectQuery.addCustomOrdering(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, orderByPropertyName)), typeDirection);
    }
}
