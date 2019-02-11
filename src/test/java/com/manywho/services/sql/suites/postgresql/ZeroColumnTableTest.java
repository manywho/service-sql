package com.manywho.services.sql.suites.postgresql;

import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Test;
import org.sql2o.Connection;

public class ZeroColumnTableTest extends ServiceFunctionalTest {
    @Test
    public void testTableWithNoColumns() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sql = "CREATE TABLE " + escapeTableName("emptytable") + "(id integer PRIMARY KEY);";
            connection.createQuery(sql).executeUpdate();
            String sql2 = "ALTER TABLE " + escapeTableName("emptytable") + " DROP COLUMN id;";
            connection.createQuery(sql2).executeUpdate();
            String sql3 = "CREATE TABLE " + escapeTableName("notemptytable") + "(id integer PRIMARY KEY, data text);";
            connection.createQuery(sql3).executeUpdate();
        }

        DefaultApiRequest.loadDataRequestAndAssertion("/metadata",
                "suites/common/metadata-empty-table-request.json",
                configurationParameters(),
                "suites/common/metadata-empty-table-response.json",
                dispatcher
        );
    }

    @After
    public void cleanDatabaseAfterEachTest() {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("emptytable", connection);
            deleteTableIfExist("notemptytable", connection);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
