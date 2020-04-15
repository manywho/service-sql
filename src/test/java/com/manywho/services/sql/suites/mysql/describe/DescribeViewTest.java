package com.manywho.services.sql.suites.mysql.describe;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

public class DescribeViewTest extends ServiceFunctionalTest {

    @Before
    public void createTable() throws ClassNotFoundException {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("deletetable") + " (id integer PRIMARY KEY);";
            connection.createQuery(sql).executeUpdate();
            String sql2 = "CREATE VIEW " + escapeTableName("emptyview") + " AS SELECT * FROM deletetable;";
            connection.createQuery(sql2).executeUpdate();
            String sql3 = "DROP TABLE " + escapeTableName("deletetable") + ";";
            connection.createQuery(sql3).executeUpdate();
            String sql4 = "CREATE TABLE " + escapeTableName("notemptytable") + " (id integer PRIMARY KEY, data text);";
            connection.createQuery(sql4).executeUpdate();
            String sql5 = "CREATE VIEW " + escapeTableName("notemptyview") + " AS SELECT * FROM notemptytable;";
            connection.createQuery(sql5).executeUpdate();
        }
    }

    @Test
    public void testViewWithNoColumns() throws Exception {

        DefaultApiRequest.describeServiceRequestAndAssertion("/metadata",
                "suites/mysql/describe/with-types/metadata-empty-view-request.json",
                configurationParameters(),
                "suites/mysql/describe/with-types/metadata-empty-view-response.json",
                dispatcher
        );
    }

    @After
    public void cleanDatabaseAfterEachTest() throws ClassNotFoundException {
        try (Connection connection = getSql2o().open()) {
            deleteViewIfExist("emptyview", connection);
            deleteTableIfExist("notemptytable", connection);
            deleteViewIfExist("notemptyview", connection);
            deleteTableIfExist("deletetable", connection);
        }
    }
}
