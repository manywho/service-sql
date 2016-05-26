package com.manywho.services.sql.managers;

import com.manywho.sdk.api.draw.elements.type.TypeElement;
import com.manywho.services.sql.ServiceConfiguration;
import com.manywho.services.sql.entities.TableMetadata;
import com.manywho.services.sql.services.DescribeService;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

public class DescribeManager {
    private DescribeService describeService;
    private MetadataManager metadataManager;

    @Inject
    public DescribeManager(DescribeService describeService, MetadataManager metadataManager) {
        this.describeService = describeService;
        this.metadataManager = metadataManager;
    }

    public List<TypeElement> getListTypeElementFromTableMetadata(ServiceConfiguration serviceConfiguration) throws Exception {
        List<TypeElement> listOfTypeElements = new ArrayList<>();

        for(TableMetadata table : metadataManager.getMetadataTables(serviceConfiguration)){
            listOfTypeElements.add(describeService.createTypeElementFromTableMetadata(table));
        }

        return listOfTypeElements;
    }
}