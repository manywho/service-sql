package com.manywho.services.sql.suites.mysql.describe;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Test;
import org.sql2o.Connection;

public class DescribeTest extends ServiceFunctionalTest {

    @Test
    public void testDescribeWithTypes() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
        String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                "id integer NOT NULL," +
                "big BOOLEAN," +
                "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                ");";

        try (Connection connection = getSql2o().open()) {
            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.describeServiceRequestAndAssertion("/metadata",
                "suites/mysql/describe/with-types/metadata1-request.json",
                configurationParameters(),
                "suites/mysql/describe/with-types/metadata1-response.json",
                dispatcher
        );
    }

    @After
    public void cleanDatabaseAfterEachTest() {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("country", connection);
        } catch (ClassNotFoundException e) {
        }
    }
}
