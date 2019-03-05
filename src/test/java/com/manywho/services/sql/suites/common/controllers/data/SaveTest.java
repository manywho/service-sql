package com.manywho.services.sql.suites.common.controllers.data;

import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Test;
import org.sql2o.Connection;

import static junit.framework.TestCase.assertEquals;

public class SaveTest extends ServiceFunctionalTest {
    @Test
    public void testCreate() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                    "id integer NOT NULL," +
                    "name character varying(255)," +
                    "description character varying(1024), " +
                    "integer_column integer, "+
                    "double_column double precision, "+
                    "numeric_column numeric(20,2), "+
                    "real_column real, "+
                    "bigint_column bigint, "+
                    "numeric_small_column numeric(26,25), "+
                    "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                    ");";
            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/create/create-request.json",
                configurationParameters(),
                "suites/common/data/save/create/create-response.json",
                dispatcher
        );
    }

    @Test
    public void testUpdate() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                    "id integer NOT NULL," +
                    "name character varying(255)," +
                    "description character varying(1024), " +
                    "integer_column integer, "+
                    "double_column double precision, "+
                    "numeric_column numeric(20,2), "+
                    "real_column real, "+
                    "bigint_column bigint, "+
                    "numeric_small_column numeric(26,25), "+
                    "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                    ");";
            connection.createQuery(sql).executeUpdate();
        }

        try (Connection connection = getSql2o().open()) {
            String sql = "INSERT INTO " + escapeTableName("country") + "(id, name, description,integer_column,double_column,numeric_column,real_column,bigint_column,numeric_small_column) VALUES ('1', 'Uruguay', 'It is a nice country', 123,0.999999999,123456789012345678.99,12233.2,12345678765432,0.9876543234500000000000000);";
            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/update/update-request.json",
                configurationParameters(),
                "suites/common/data/save/update/update-response.json",
                dispatcher
        );
    }

    @Test
    public void testUpdateMissingId () throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");

        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("testtable") + " (id integer primary key auto_increment NOT NULL, data text);";
            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/update/update-missing-request.json",
                configurationParameters(),
                "suites/common/data/save/update/update-missing-response.json",
                dispatcher
        );
    }

    private void createTestTable() throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("testtable") + " (id integer primary key auto_increment NOT NULL, data text);";
            connection.createQuery(sql).executeUpdate();
        }
    }

    private void insertToTestTable(String data) throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "INSERT INTO " + escapeTableName("testtable") + " (data) VALUES ('" + data + "');";
            connection.createQuery(sql).executeUpdate();
        }
    }

    // thinking about taking the auto increment off the id so we manually set it
    // and 1) know what it will be instead of relying on what number the (SQL)
    // engine decides and 2) we then don't need to concern ourselves with how to
    // auto increment and check the numbers in this test
    private void insertToTestTable(int id, String data) throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "INSERT INTO " + escapeTableName("testtable") + " (id, data) VALUES ('" + id + "', '" + data + "');";
            connection.createQuery(sql).executeUpdate();
        }
    }

    private void assertCountOfTestTable (int expected) throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT count(*) From " + escapeTableName("testtable") + ";";
            int found = connection.createQuery(sql).executeScalar(Integer.class);
            assertEquals(expected, found);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void createMulitipleTest() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");

        createTestTable();

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/create/create-multiple-request.json",
                configurationParameters(),
                "suites/common/data/save/create/create-multiple-response.json",
                dispatcher
        );
    }

    @Test
    public void updateMulitipleTest() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");

        createTestTable();

        insertToTestTable("first");
        insertToTestTable("second");


        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/update/update-multiple-request.json",
                configurationParameters(),
                "suites/common/data/save/update/update-multiple-response.json",
                dispatcher
        );
    }

    @Test
    public void updateAndCreateTest() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");

        createTestTable();

        insertToTestTable("first");

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/createAndUpdate/create-update-request.json",
                configurationParameters(),
                "suites/common/data/save/createAndUpdate/create-update-response.json",
                dispatcher
        );
    }

    @Test
    public void updateMultipleMissingId() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");

        createTestTable();

        insertToTestTable("first");

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/update/update-multiple-missing-request.json",
                configurationParameters(),
                "suites/common/data/save/update/update-multiple-missing-response.json",
                dispatcher
        );
    }

    @After
    public void cleanDatabaseAfterEachTest() {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("country", connection);
            deleteTableIfExist("testtable", connection);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
