package com.manywho.services.sql.suites.mysql.data;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.json.JSONException;
import java.io.IOException;
import java.net.URISyntaxException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

public class DateTest extends ServiceFunctionalTest {

    @Before
    public void setupDatabase() {
        DbConfigurationTest.setPropertiesIfNotInitialized("mysql");
    }

    @Test
    public void testUpdateDate() throws ClassNotFoundException, JSONException, IOException, URISyntaxException {
        try (Connection connection = getSql2o().open()) {
            String sqlCreate = "CREATE TABLE " + escapeTableName("datetest") +
                    "(" +
                    "id integer NOT NULL," +
                    "date_only DATE," +
                    "CONSTRAINT datetest_id_pk PRIMARY KEY (id)" +
                    ");";

            connection.createQuery(sqlCreate).executeUpdate();

            String sql = "INSERT INTO " + escapeTableName("datetest") +"(id, date_only) VALUES " +
                    "('1', '2020-01-01');";

            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.saveDataRequestAndAssertion("/data",
                "suites/mysql/dates/save/request-dates.json",
                configurationParameters(),
                "suites/mysql/date/save/response-dates.json",
                dispatcher
        );
    }


    @After
    public void cleanDatabaseAfterEachTest() throws ClassNotFoundException {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("datetest", connection);
        }
    }
}
