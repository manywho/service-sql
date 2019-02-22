package com.manywho.services.sql.suites.common.controllers.data;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Test;
import org.sql2o.Connection;

import static junit.framework.TestCase.assertEquals;

public class DeleteTest extends ServiceFunctionalTest {

    @Test
    public void testDeleteDataByExternalId() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sqlCreateTable = "CREATE TABLE " + escapeTableName("city") +
                    "(" +
                    "cityname character varying(255)," +
                    "countryname character varying(255)," +
                    "description character varying(1024)," +
                    "CONSTRAINT city_pk PRIMARY KEY (cityname, countryname)" +
                    ");";
            connection.createQuery(sqlCreateTable).executeUpdate();

            String sqlInsert = "INSERT INTO " + escapeTableName("city")+ "(cityname, countryname) VALUES ('Montevideo', 'Uruguay');";
            connection.createQuery(sqlInsert).executeUpdate();
        }

        DefaultApiRequest.loadDataRequestAndAssertion("/data/delete",
                "suites/common/data/delete/request.json",
                configurationParameters(),
                "suites/common/data/delete/response.json",
                dispatcher
        );

        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT count(cityname) From " + escapeTableName("city")+ " WHERE cityname = 'Montevideo' and countryname ='Uruguay';";
            int found = connection.createQuery(sql).executeScalar(Integer.class);
            assertEquals(0, found);
        }
    }

    @Test
    public void testDeleteMultipleDataByExternalId() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sqlCreateTable = "CREATE TABLE " + escapeTableName("testtable") +
                    "(" +
                    "id integer primary key auto_increment," +
                    "data text" +
                    ");";
            connection.createQuery(sqlCreateTable).executeUpdate();

            String sqlInsert = "INSERT INTO " + escapeTableName("testtable")+ " (data) VALUES ('test1'), ('test2');";
            connection.createQuery(sqlInsert).executeUpdate();
        } catch (Exception e) {
            throw e;
        }

        // inserted two lines make sure they're there
        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT count(*) From " + escapeTableName("testtable")+ ";";
            int found = connection.createQuery(sql).executeScalar(Integer.class);
            assertEquals(2, found);
        } catch (Exception e) {
            throw e;
        }

        // delete both by external id
        DefaultApiRequest.loadDataRequestAndAssertion("/data/delete",
                "suites/common/data/delete/request-multiple.json",
                configurationParameters(),
                "suites/common/data/delete/response.json",
                dispatcher
        );

        // make sure they're both gone
        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT count(*) From " + escapeTableName("testtable")+ ";";
            int found = connection.createQuery(sql).executeScalar(Integer.class);
            assertEquals(0, found);
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    public void testDeleteMultipleMissingDataByExternalId() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sqlCreateTable = "CREATE TABLE " + escapeTableName("testtable") +
                    "(" +
                    "id integer primary key auto_increment," +
                    "data text" +
                    ");";
            connection.createQuery(sqlCreateTable).executeUpdate();

            String sqlInsert = "INSERT INTO " + escapeTableName("testtable")+ "(data) VALUES ('test1');";
            connection.createQuery(sqlInsert).executeUpdate();
        } catch (Exception e) {
            throw e;
        }

        // only inserted one, make sure it's there
        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT count(*) From " + escapeTableName("testtable")+ ";";
            int found = connection.createQuery(sql).executeScalar(Integer.class);
            assertEquals(1, found);
        } catch (Exception e) {
            throw e;
        }

        // try delete it and another one
        DefaultApiRequest.loadDataRequestAndAssertion("/data/delete",
                "suites/common/data/delete/request-multiple.json",
                configurationParameters(),
                "suites/common/data/delete/response.json",
                dispatcher
        );

        // check that it did fail and we've still got our one line
        try (Connection connection = getSql2o().open()) {
            String sql = "SELECT count(*) From " + escapeTableName("testtable")+ ";";
            int found = connection.createQuery(sql).executeScalar(Integer.class);
            assertEquals(1, found);
        } catch (Exception e) {
            throw e;
        }
    }

    @After
    public void cleanDatabaseAfterEachTest() {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("city", connection);
            deleteTableIfExist("testtable", connection);
        } catch (ClassNotFoundException e) {
        }
    }
}
