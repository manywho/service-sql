package com.manywho.services.sql.services.filter;

import com.google.common.base.Strings;
import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.custom.mysql.MysLimitClause;
import com.healthmarketscience.sqlbuilder.custom.postgresql.PgLimitClause;
import com.healthmarketscience.sqlbuilder.custom.postgresql.PgOffsetClause;

import com.manywho.sdk.api.ComparisonType;
import com.manywho.sdk.api.ContentType;
import com.manywho.sdk.api.CriteriaType;
import com.manywho.sdk.api.run.elements.type.ListFilter;
import com.manywho.sdk.api.run.elements.type.ListFilterWhere;
import com.manywho.sdk.api.run.elements.type.ObjectDataTypeProperty;
import com.manywho.services.sql.entities.DatabaseType;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.utilities.ScapeForTablesUtil;

import java.sql.JDBCType;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class QueryFilterConditions {
    public void addSearch(SelectQuery selectQuery, QueryPreparer preparer, String search, List<ObjectDataTypeProperty> listProperties,
                          HashMap<String, String> columns, DatabaseType databaseType, List<Object> placeHolderParameters) {

        if (Strings.isNullOrEmpty(search)) {
            return;
        }

        // Find all the string-ish columns, so we can compare the search criteria against them
        Map<String, String> stringishColumns = columns.entrySet().stream()
                .filter(column -> column.getValue().equals(JDBCType.VARCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.NVARCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.CHAR.getName()) ||
                                    column.getValue().equals(JDBCType.NCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.LONGVARCHAR.getName()) ||
                                    column.getValue().equals(JDBCType.LONGNVARCHAR.getName()))
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));

        // %search%
        String searchTerm = String.format("%%%s%%", search);
        ComboCondition searchCondition = new ComboCondition(ComboCondition.Op.OR);
        SqlObject placeHolder = preparer.getNewMultiPlaceHolder();

        for (ObjectDataTypeProperty property : listProperties) {
            if (stringishColumns.containsKey(property.getDeveloperName())) {
                searchCondition.addCondition(BinaryCondition.like(
                        new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, property.getDeveloperName())),
                        placeHolder
                ));
                placeHolderParameters.add(searchTerm);
            }
        }

        selectQuery.addCondition(searchCondition);
    }

    public void addWhere(SelectQuery selectQuery, QueryPreparer preparer, List<ListFilterWhere> whereList, ComparisonType comparisonType,
                         DatabaseType databaseType, TableMetadata tableMetadata, List<Object> placeHolderParameters) {

        ArrayList<Condition> conditions = new ArrayList<>();
        if(whereList == null) return;

        String comparisonTypeLocal = "";

        for (ListFilterWhere filterWhere: whereList) {
            conditions.add(getConditionFromFilterElement(preparer, filterWhere, databaseType, tableMetadata, placeHolderParameters));
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

    private Object parseParameterObject(TableMetadata tableMetadata, String columnName, String contentValue) {
        ContentType contentType = tableMetadata.getColumns().getOrDefault(columnName, ContentType.String);

        if (contentValue == null) {
            return null;
        }

        switch (contentType) {
            case Boolean:
                return Boolean.valueOf(contentValue);
            case Number:
                try {
                    return NumberFormat.getInstance().parse(contentValue);
                } catch (ParseException e) {
                    throw new RuntimeException(String.format("The value of %s is not a valid number", columnName));
                }
            case DateTime:
                return OffsetDateTime.parse(contentValue);
            default:
                // this special case is for postgres uuid value
                if ("uuid".equals(tableMetadata.getColumnsDatabaseType().get(columnName))) {
                    return UUID.fromString(contentValue);
                }

                return contentValue;
        }

    }

    private Condition getConditionFromFilterElement(QueryPreparer preparer, ListFilterWhere filterWhere, DatabaseType databaseType,
                                                    TableMetadata tableMetadata, List<Object> placeHolderParameters) {
        Object object = null;
        SqlObject placeHolder = null;

        //we don't use the value for is_empty because it will be transformed IS NOT NULL or IS NULL
        if (filterWhere.getCriteriaType() != CriteriaType.IsEmpty) {
            object = parseParameterObject(tableMetadata, filterWhere.getColumnName(), filterWhere.getContentValue());
            placeHolder = preparer.getNewMultiPlaceHolder();
        }

        switch (filterWhere.getCriteriaType()) {
            case Equal:
                placeHolderParameters.add(object);
               return BinaryCondition.equalTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder);
            case NotEqual:
                placeHolderParameters.add(object);
               return BinaryCondition.notEqualTo(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder);
            case GreaterThan:
                placeHolderParameters.add(object);
               return BinaryCondition.greaterThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder, false);
            case GreaterThanOrEqual:
                placeHolderParameters.add(object);
               return BinaryCondition.greaterThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder, true);
            case LessThan:
                placeHolderParameters.add(object);
               return BinaryCondition.lessThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder, false);
            case LessThanOrEqual:
                placeHolderParameters.add(object);
               return BinaryCondition.lessThan(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder, true);
            case Contains:
                placeHolderParameters.add(prepareLike("%%%s%%", fixCharacterEscapes(filterWhere.getContentValue())));
               return BinaryCondition.like(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder);
            case StartsWith:
                placeHolderParameters.add(prepareLike("%s%%", fixCharacterEscapes(filterWhere.getContentValue())));
               return BinaryCondition.like(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder);
            case EndsWith:
                placeHolderParameters.add(prepareLike("%%%s", fixCharacterEscapes(filterWhere.getContentValue())));
               return BinaryCondition.like(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())), placeHolder);
            case IsEmpty:
                if (Strings.isNullOrEmpty(filterWhere.getContentValue()) == false && filterWhere.getContentValue().toLowerCase().equals("true")) {
                    return UnaryCondition.isNull(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())));
                } else {
                    return UnaryCondition.isNotNull(new CustomSql(ScapeForTablesUtil.scapeCollumnName(databaseType, filterWhere.getColumnName())));
                }
            default:
                break;
        }
        return null;
    }

    private String fixCharacterEscapes(String value) {
        if(value.contains("'"))
        {
            value = value.replace("'", "''");
        }
        return value;
    }

    private String prepareLike(String pattern, String value) {
        if (value == null || "".equals(value)) {
            return "%%";
        }

        return String.format(pattern, value);
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
