package com.manywho.services.sql.suites.postgresql.data;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

public class SearchTest extends ServiceFunctionalTest {

    @Before
    public void setupDatabase() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sql = String.format(
                "CREATE TABLE %s (" +
                "id INT NOT NULL PRIMARY KEY," +
                "col_char CHAR(10)," +
                "col_varchar VARCHAR(10)," +
                "col_text TEXT" +
                ")",
            escapeTableName("searchtest"));

            connection.createQuery(sql).executeUpdate();

            String sql2 = String.format("INSERT INTO %s (id, col_char, col_varchar, col_text) VALUES " +
                "(1, 'aaaa', 'xxxx', 'xxxx')," +
                "(2, 'xxxx', 'bbbb', 'xxxx')," +
                "(3, 'xxxx', 'xxxx', 'cccc')," +
                "(4, 'zzzz', 'zzzz', 'zzzz')",
                escapeTableName("searchtest"));

            connection.createQuery(sql2).executeUpdate();
        }
    }

    @Test
    public void testSearchChar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/search/char-request.json",
                configurationParameters(),
                "suites/postgresql/search/char-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchVarchar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/search/varchar-request.json",
                configurationParameters(),
                "suites/postgresql/search/varchar-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchText() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/search/text-request.json",
                configurationParameters(),
                "suites/postgresql/search/text-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchNoResults() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/search/noresults-request.json",
                configurationParameters(),
                "suites/postgresql/search/noresults-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchAllResults() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/search/allresults-request.json",
                configurationParameters(),
                "suites/postgresql/search/allresults-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchXxxx() throws Exception {
        // Three rows - all bar the zzzz row
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/search/xxxx-request.json",
                configurationParameters(),
                "suites/postgresql/search/xxxx-response.json",
                dispatcher
        );
    }

    @After
    public void cleanDatabaseAfterEachTest() throws ClassNotFoundException {
        try (Connection connection = getSql2o().open()) {
             deleteTableIfExist("searchtest", connection);
        }
    }
}
