package com.manywho.services.sql.suites.postgresql.data;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sql2o.Connection;

public class LoadSpecialForPostgresTest extends ServiceFunctionalTest {

    @Before
    public void createTable() throws ClassNotFoundException {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sqlCreateTable = "CREATE TABLE " + escapeTableName("country") + "(" +
                    "id int NOT NULL," +
                    "uuid1 uuid NULL, " +
                    "created_at timestamp NULL," +
                    "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                    ");";

            connection.createQuery(sqlCreateTable).executeUpdate();
        }
    }

    @Test
    public void testLoadWithIsNullAndUuid() throws Exception {
        try (Connection connection = getSql2o().open()) {
            String sql = "INSERT INTO " + escapeTableName("country")+"(id, uuid1) VALUES ('1', '123e4567-e89b-12d3-a456-426655440000');";
            connection.createQuery(sql).executeUpdate();
        }
        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/postgresql/dates/timestamp/request-dates.json",
                configurationParameters(),
                "suites/postgresql/dates/timestamp/response-dates.json",
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
