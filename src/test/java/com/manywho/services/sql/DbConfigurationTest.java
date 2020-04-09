package com.manywho.services.sql;

public class DbConfigurationTest {
    static public boolean initialized = false;
    static public String portForTest;
    static public String databaseTypeForTest;
    static public String databaseNameForTest;
    static public String schemaForTest ;
    static public String hostForTest;

    static public String userName;
    static public String password;

    static public void setPorperties(String databaseType) {
        DbConfigurationTest.initialized = true;

        DbConfigurationTest.portForTest = System.getenv(databaseType + "_port");
        DbConfigurationTest.databaseTypeForTest = System.getenv(databaseType + "_databaseType");
        DbConfigurationTest.schemaForTest = System.getenv(databaseType + "_schema");
        DbConfigurationTest.hostForTest = System.getenv(databaseType + "_host");
        DbConfigurationTest.databaseNameForTest = System.getenv(databaseType + "_databaseName");

        DbConfigurationTest.userName = System.getenv(databaseType + "_userName");
        DbConfigurationTest.password = System.getenv(databaseType + "_password");
    }

    static public void setPropertiesIfNotInitialized(String databaseType) {
        if(!DbConfigurationTest.initialized) {
            initialized = true;

            DbConfigurationTest.portForTest = System.getenv(databaseType + "_port");
            DbConfigurationTest.databaseTypeForTest = System.getenv(databaseType + "_databaseType");
            DbConfigurationTest.schemaForTest = System.getenv(databaseType + "_schema");
            DbConfigurationTest.hostForTest = System.getenv(databaseType + "_host");
            DbConfigurationTest.databaseNameForTest = System.getenv(databaseType + "_databaseName");

            DbConfigurationTest.userName = System.getenv(databaseType + "_userName");
            DbConfigurationTest.password = System.getenv(databaseType + "_password");
        }
    }
}
