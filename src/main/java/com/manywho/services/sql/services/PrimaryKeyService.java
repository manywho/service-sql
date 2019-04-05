package com.manywho.services.sql.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.manywho.sdk.api.run.elements.type.MObject;
import com.manywho.sdk.api.run.elements.type.Property;
import org.apache.commons.codec.binary.Base64;
import java.util.HashMap;
import java.util.Map;

public class PrimaryKeyService {

    private ObjectMapper objectMapper;

    public PrimaryKeyService() {
        objectMapper = new ObjectMapper();
    }

    public HashMap<String, String> deserializePrimaryKey(String serializedPrimaryKey) {
        try {
            byte[] decoded = Base64.decodeBase64(serializedPrimaryKey.getBytes());
            return objectMapper.readValue(new String(decoded, "UTF-8"), new TypeReference<HashMap<String,Object>>() {});

        } catch (Exception e) {
            throw new RuntimeException("The primary key is not valid. It should be a JSON object, then Base64 encoded, e.g. {\"id\": 1} would become eyJpZCI6MX0=", e);
        }
    }

    public String serializePrimaryKey(HashMap<String, String> primaryKeys) {
        try {
            return new String(Base64.encodeBase64(objectMapper.writeValueAsString(primaryKeys).getBytes()), "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("error when serialize primary key", e);
        }
    }

    /**
     * For use when one has updated the MObject primary key and you need to re-generate the primary key from the MObject
     *
     * @param pk the original String, String HashMap of the primary key to replace data into
     * @param object the MObject to take the new data from
     * @return new primary key String, String HasMap
     */
    public HashMap<String, String> updateFromObject (HashMap<String, String> pk, MObject object) {
        for (Map.Entry<String, String> element : pk.entrySet()) {
            for (Property prop : object.getProperties()) {
                if (prop.getDeveloperName().equals(element.getKey())) {
                    element.setValue(prop.getContentValue());
                    break;
                }
            }
        }
        return pk;
    }
}
