package com.manywho.services.sql.suites.postgresql.data;

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
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");

        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                    "name character varying(255)," +
                    "description character varying(1024), " +
                    "id SERIAL NOT NULL," +
                    "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                    ");";
            connection.createQuery(sql).executeUpdate();
        }
    }

    @Test
    public void testCreateDataWithAutoIncrement() throws Exception {

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/postgresql/autoincrement/create-request.json",
                configurationParameters(),
                "suites/postgresql/autoincrement/create-response.json",
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
                "suites/postgresql/update-autoincrement/update-request.json",
                configurationParameters(),
                "suites/postgresql/update-autoincrement/update-response.json",
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
