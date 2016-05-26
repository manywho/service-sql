package com.manywho.services.sql.managers;

import com.manywho.services.sql.ServiceConfiguration;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.services.ConnectionService;
import com.manywho.services.sql.services.MetadataService;
import org.sql2o.Connection;

import javax.inject.Inject;
import java.sql.DatabaseMetaData;
import java.util.List;

public class MetadataManager {
    private ConnectionService connectionService;
    private MetadataService metadataService;

    @Inject
    public MetadataManager(ConnectionService connectionService, MetadataService metadataService){
        this.connectionService = connectionService;
        this.metadataService = metadataService;
    }

    public List<TableMetadata> getMetadataTables(ServiceConfiguration serviceConfiguration) throws Exception {

        try (Connection con = connectionService.getConnection(serviceConfiguration).open()) {
            DatabaseMetaData metaData = con.getJdbcConnection().getMetaData();

            return metadataService.getTablesMetadata(
                    serviceConfiguration.getDatabaseName(),
                    metaData
            );
        }
    }

    public TableMetadata getMetadataTable(ServiceConfiguration serviceConfiguration, String tableName) throws Exception {

        try (Connection con = connectionService.getConnection(serviceConfiguration).open()) {
            DatabaseMetaData metaData = con.getJdbcConnection().getMetaData();

            List<TableMetadata> tableMetadataLis = metadataService.getTablesMetadata(
                    serviceConfiguration.getDatabaseName(),
                    metaData,
                    tableName
            );

            if(tableMetadataLis.size() == 1) {

                return tableMetadataLis.get(0);
            } else {

                throw new Exception ("table" + tableName + "not found");
            }
        }
    }
}