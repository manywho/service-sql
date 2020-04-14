package com.manywho.services.sql.suites.mysql.describe;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

public class DescribeTest extends ServiceFunctionalTest {

    @Before
    public void createTable() throws ClassNotFoundException {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
        String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                "id integer NOT NULL," +
                "big BOOLEAN," +
                "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                ");";

        try (Connection connection = getSql2o().open()) {
            connection.createQuery(sql).executeUpdate();
        }
    }

    @Test
    public void testDescribeWithTypes() throws Exception {
        DefaultApiRequest.describeServiceRequestAndAssertion("/metadata",
                "suites/mysql/describe/with-types/metadata1-request.json",
                configurationParameters(),
                "suites/mysql/describe/with-types/metadata1-response.json",
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
