package com.manywho.services.sql.services;

import com.google.common.collect.Lists;
import com.manywho.sdk.api.ContentType;
import com.manywho.sdk.api.draw.elements.type.TypeElement;
import com.manywho.sdk.api.draw.elements.type.TypeElementBinding;
import com.manywho.sdk.api.draw.elements.type.TypeElementProperty;
import com.manywho.sdk.api.draw.elements.type.TypeElementPropertyBinding;
import com.manywho.sdk.api.run.elements.type.Property;
import com.manywho.sdk.services.types.TypePropertyInvalidException;
import com.manywho.services.sql.entities.TableMetadata;
import microsoft.sql.DateTimeOffset;
import org.sql2o.data.Table;

import javax.inject.Inject;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;
import java.util.stream.Collectors;

public class DescribeService {
    private AliasService aliasService;

    @Inject
    public DescribeService(AliasService aliasService){
        this.aliasService = aliasService;
    }

    public TypeElement createTypeElementFromTableMetadata(TableMetadata tableMetadata) throws SQLException {
        HashMap<String, ContentType> metadataProperties = tableMetadata.getColumnsAndContentTypeWithAlias();

        List<TypeElementProperty> properties = Lists.newArrayList();
        List<TypeElementPropertyBinding> propertyBindings = Lists.newArrayList();

        for(Map.Entry<String, ContentType> property: metadataProperties.entrySet()) {
            properties.add(new TypeElementProperty(property.getKey(), property.getValue()));
            propertyBindings.add(new TypeElementPropertyBinding(property.getKey(), property.getKey(),
                    aliasService.getColumnDatabaseType(tableMetadata, property.getKey())));
        }

        List<TypeElementBinding> bindings = Lists.newArrayList();
        bindings.add(new TypeElementBinding(tableMetadata.getTableName(), "The binding for " + tableMetadata.getTableName(), tableMetadata.getTableName(), propertyBindings));

        return new TypeElement(tableMetadata.getTableName(), properties, bindings);
    }

    public List<Property> createProperties(Table table, TableMetadata tableMetadata) {
        HashMap<String, ContentType> columnsContentType = tableMetadata.getColumns();
        List<Property> properties = new ArrayList<>();

        properties.addAll(
                table.columns().stream()
                        .filter(column1 -> columnsContentType.get(column1.getName()) != null)
                        .map(column -> new Property(column.getName(), "", columnsContentType.get(column.getName())))
                        .collect(Collectors.toList()));

        return properties;
    }

    public void populateProperty(String propertyName, String propertyValue, List<Property> propertyList) {
        propertyList.stream()
                .filter(p -> Objects.equals(p.getDeveloperName(), propertyName))
                .forEach(p-> p.setContentValue(propertyValue));
    }

    public void populatePropertyDate(String propertyName, Object propertyValue, List<Property> propertyList) {;

        propertyList.stream()
                .filter(p -> Objects.equals(p.getDeveloperName(), propertyName))
                .forEach(p-> p.setContentValue(serializeDate(p.getDeveloperName(), propertyValue)));
    }

    private static String serializeDate(String property, Object object) {
        if (object == null){
            return null;
        } else if (object instanceof TemporalAccessor) {

            return java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.format((TemporalAccessor) object);
        } else if (object instanceof DateTimeOffset) {
            java.sql.Timestamp timestamp =  ((DateTimeOffset) object).getTimestamp();
            Integer minutesOffset =  ((DateTimeOffset) object).getMinutesOffset();
            OffsetDateTime offsetDateTime = OffsetDateTime.of(timestamp.toLocalDateTime(),
                    ZoneOffset.ofTotalSeconds(minutesOffset*60));

            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(offsetDateTime);
        } else if (object instanceof java.sql.Date) {
            ZonedDateTime dateTime = ((java.sql.Date) object).toLocalDate().atStartOfDay(ZoneId.of("UTC"));

            return java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dateTime);
        } else if (object instanceof Date) {
            Instant instant = ((Date) object).toInstant();
            OffsetDateTime dateTime = OffsetDateTime.ofInstant(instant, ZoneId.of("UTC"));

            return java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dateTime);
        }

        throw new TypePropertyInvalidException(property, "Date");
    }
}
