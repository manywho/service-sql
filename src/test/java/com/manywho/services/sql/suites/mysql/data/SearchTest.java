package com.manywho.services.sql.suites.mysql.data;

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
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
        try (Connection connection = getSql2o().open()) {
            String sql = String.format(
                "CREATE TABLE %s (" +
                "id INT NOT NULL PRIMARY KEY," +
                "col_char CHAR(10)," +
                "col_varchar VARCHAR(10)," +
                "col_tinytext TINYTEXT," +
                "col_text TEXT," +
                "col_mediumtext MEDIUMTEXT," +
                "col_longtext LONGTEXT" +
                ")",
            escapeTableName("searchtest"));

            connection.createQuery(sql).executeUpdate();

            String sql2 = String.format("INSERT INTO %s (id, col_char, col_varchar, col_tinytext, col_text, col_mediumtext, col_longtext) VALUES " +
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
                "suites/mysql/search/char-request.json",
                configurationParameters(),
                "suites/mysql/search/char-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchVarchar() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/varchar-request.json",
                configurationParameters(),
                "suites/mysql/search/varchar-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchTinytext() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/tinytext-request.json",
                configurationParameters(),
                "suites/mysql/search/tinytext-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchText() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/text-request.json",
                configurationParameters(),
                "suites/mysql/search/text-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchMediumtext() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/mediumtext-request.json",
                configurationParameters(),
                "suites/mysql/search/mediumtext-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchLongtext() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/longtext-request.json",
                configurationParameters(),
                "suites/mysql/search/longtext-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchNoResults() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/noresults-request.json",
                configurationParameters(),
                "suites/mysql/search/noresults-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchAllResults() throws Exception {
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/allresults-request.json",
                configurationParameters(),
                "suites/mysql/search/allresults-response.json",
                dispatcher
        );
    }

    @Test
    public void testSearchXxxx() throws Exception {
        // Five rows - all bar the zzzz row
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/mysql/search/xxxx-request.json",
                configurationParameters(),
                "suites/mysql/search/xxxx-response.json",
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
