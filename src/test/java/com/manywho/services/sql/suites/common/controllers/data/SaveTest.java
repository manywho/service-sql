package com.manywho.services.sql.suites.common.controllers.data;

import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.exceptions.RecordNotFoundException;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Test;
import org.junit.Assert;
import org.sql2o.Connection;

import static junit.framework.TestCase.assertEquals;

public class SaveTest extends ServiceFunctionalTest {
    @Test
    public void testCreate() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
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
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
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

        createTestTable();
        // ::TODO:: fix annoying JBoss exception hack below...
        // stupid hack to check the exception as JBoss is burying my exception in an unhandled exception. I tried
        // setting (expect=RecordNotShownException.class) on the @Test however, JBoss is finding this as an unhandled
        // exception and maybe wrapping it in a unhandled? so we have to catch it and check it's an instance of!
        try {
            DefaultApiRequest.saveDataRequestAndAssertion("/data",
                    "suites/common/data/save/update/update-missing-request.json",
                    configurationParameters(),
                    "suites/common/data/save/update/update-missing-response.json",
                    dispatcher
            );
        } catch (Exception ex) {
            if (ex.getCause() instanceof RecordNotFoundException) {
                assertCountOfTestTable(0);
                return;
            }
            ex.printStackTrace();
            Assert.fail("Unexpected exception: " +ex);
            throw ex;
        }
        Assert.fail("Expected RecordNotFoundException");
    }

    private void createTestTable() throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("testtable") + " (id integer primary key NOT NULL, data text);";
            connection.createQuery(sql).executeUpdate();
        }
    }

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

    private void assertIndexEqualsTestTable (int index, String expected) throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT data from " + escapeTableName("testtable") + " where id = '"+index+"';";
            String response = connection.createQuery(sql).executeScalar(String.class);
            assertEquals(expected, response);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void createMultipleTest() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");

        createTestTable();

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/create/create-multiple-request.json",
                configurationParameters(),
                "suites/common/data/save/create/create-multiple-response.json",
                dispatcher
        );
    }

    @Test
    public void updateMultipleTest() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");

        createTestTable();

        insertToTestTable(1,"first");
        insertToTestTable(2,"second");


        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/common/data/save/update/update-multiple-request.json",
                configurationParameters(),
                "suites/common/data/save/update/update-multiple-response.json",
                dispatcher
        );
    }

    @Test
    public void updateAndCreateTest() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");

        createTestTable();

        insertToTestTable(1,"first");

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

        insertToTestTable(1,"first");

        try {
            DefaultApiRequest.saveDataRequestAndAssertion("/data",
                    "suites/common/data/save/update/update-multiple-missing-request.json",
                    configurationParameters(),
                    "suites/common/data/save/update/update-multiple-missing-response.json",
                    dispatcher
            );
        } catch (Exception ex) {
            if (ex.getCause() instanceof RecordNotFoundException) {
                assertCountOfTestTable(1);
                assertIndexEqualsTestTable(1, "first");
                return;
            }
            ex.printStackTrace();
            Assert.fail("Unexpected exception: " +ex);
            throw ex;
        }
        Assert.fail("Expected RecordNotFoundException");
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
