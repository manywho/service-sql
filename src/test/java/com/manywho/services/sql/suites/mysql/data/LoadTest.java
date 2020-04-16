package com.manywho.services.sql.suites.mysql.data;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

import java.io.IOException;
import java.net.URISyntaxException;

public class LoadTest extends ServiceFunctionalTest {

    @Before
    public void setupTableCountryTable() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
        String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                "id integer NOT NULL," +
                "big BOOLEAN," +
                "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                ");";

        String sql2 = "INSERT INTO " + escapeTableName("country") + "(id, big) VALUES " +
                "('1', true)," +
                "('2', false);";

        try (Connection connection = getSql2o().open()) {
            connection.createQuery(sql).executeUpdate();
            connection.createQuery(sql2).executeUpdate();
        }
    }

    @Test
    public void testLoadWithBooleanAndIntFilter() throws JSONException, IOException, URISyntaxException {

        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/load-bool-int/load-request.json",
                configurationParameters(),
                "suites/mysql/load-bool-int/load-response.json",
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
