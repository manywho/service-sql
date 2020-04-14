package com.manywho.services.sql.suites.sqlserver.data;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

public class AutoIncrementTest extends ServiceFunctionalTest {
    @Before
    public void createTable() throws ClassNotFoundException {
        DbConfigurationTest.setPropertiesIfNotInitialized("sqlserver");

        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                    "name character varying(255)," +
                    "id INT NOT NULL  IDENTITY (1,1) PRIMARY KEY," +
                    "description character varying(1024), " +
                    ");";
            connection.createQuery(sql).executeUpdate();
        }
    }

    @Test
    public void testCreateDataWithAutoIncrement() throws Exception {
        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/mysql/autoincrement/create/create-request.json",
                configurationParameters(),
                "suites/mysql/autoincrement/create/create-response.json",
                dispatcher
        );
    }

    @Test
    public void testUpdateAutoincrement() throws Exception {

        try (Connection connection = getSql2o().open()) {
            String sql = "INSERT INTO " + escapeTableName("country") + "( name, description) VALUES ( 'Uruguay', 'It is a nice country');";
            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/sqlserver/autoincrement/update/update-request.json",
                configurationParameters(),
                "suites/sqlserver/autoincrement/update/update-response.json",
                dispatcher
        );
    }


    @After
    public void cleanDatabaseAfterEachTest() throws ClassNotFoundException {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("country", connection);
        }
    }
}
