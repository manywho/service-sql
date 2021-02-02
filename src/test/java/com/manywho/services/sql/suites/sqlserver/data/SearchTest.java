package com.manywho.services.sql.suites.sqlserver.data;

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
        DbConfigurationTest.setPropertiesIfNotInitialized("sqlserver");
        try (Connection connection = getSql2o().open()) {
            String sql = String.format(
                "CREATE TABLE %s (" +
                "id INT NOT NULL PRIMARY KEY," +
                "col_char CHAR(10)," +
                "col_nchar NCHAR(10)," +
                "col_varchar VARCHAR(10)," +
                "col_nvarchar NVARCHAR(10)," +
                "col_text TEXT," +
                "col_ntext NTEXT" +
                ")",
            escapeTableName("searchtest"));

            connection.createQuery(sql).executeUpdate();

            String sql2 = String.format("INSERT INTO %s (id, col_char, col_nchar, col_varchar, col_nvarchar, col_text, col_ntext) VALUES " +
                "(1, 'aaaa', 'xxxx', 'xxxx', 'xxxx', 'xxxx', 'xxxx')," +
                "(2, 'xxxx', 'bbbb', 'xxxx', 'xxxx', 'xxxx', 'xxxx')," +
                "(3, 'xxxx', 'xxxx', 'cccc', 'xxxx', 'xxxx', 'xxxx')," +
                "(4, 'xxxx', 'xxxx', 'xxxx', 'dddd', 'xxxx', 'xxxx')," +
                "(5, 'xxxx', 'xxxx', 'xxxx', 'xxxx', 'eeee', 'xxxx')," +
                "(6, 'xxxx', 'xxxx', 'xxxx', 'xxxx', 'xxxx', 'ffff')," +
                "(7, 'zzzz', 'zzzz', 'zzzz', 'zzzz', 'zzzz', 'zzzz')",
                escapeTableName("searchtest"));

            connection.createQuery(sql2).executeUpdate();
        }
    }

    @Test
    public void testSearchChar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/char-request.json",
                configurationParameters(),
                "suites/sqlserver/search/char-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchNchar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/nchar-request.json",
                configurationParameters(),
                "suites/sqlserver/search/nchar-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchVarchar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/varchar-request.json",
                configurationParameters(),
                "suites/sqlserver/search/varchar-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchNvarchar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/nvarchar-request.json",
                configurationParameters(),
                "suites/sqlserver/search/nvarchar-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchText() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/text-request.json",
                configurationParameters(),
                "suites/sqlserver/search/text-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchNtext() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/ntext-request.json",
                configurationParameters(),
                "suites/sqlserver/search/ntext-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchNoResults() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/noresults-request.json",
                configurationParameters(),
                "suites/sqlserver/search/noresults-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchAllResults() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/allresults-request.json",
                configurationParameters(),
                "suites/sqlserver/search/allresults-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchXxxx() throws Exception {
        // Five rows - all bar the zzzz row
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/sqlserver/search/xxxx-request.json",
                configurationParameters(),
                "suites/sqlserver/search/xxxx-response.json",
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
