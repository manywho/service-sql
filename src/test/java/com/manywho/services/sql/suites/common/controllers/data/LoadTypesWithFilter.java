package com.manywho.services.sql.suites.common.controllers.data;

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

public class LoadTypesWithFilter extends ServiceFunctionalTest {

    @Before
    public void setupTableCountryTable() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        try (Connection connection = getSql2o().open()) {
            String sqlCreateTable = "CREATE TABLE " + escapeTableName("country") + "("+
                            "id integer NOT NULL,"+
                            "name character varying(255)," +
                            "description character varying(1024) null," +
                            "date_at date null," +
                            "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                        ");";

            connection.createQuery(sqlCreateTable).executeUpdate();
        }
    }

    @Test
    public void testLoadTypesUsingWhere() throws ClassNotFoundException, JSONException, IOException, URISyntaxException {
        try (Connection connection = getSql2o().open()) {
            String sql = "INSERT INTO "+ escapeTableName("country")+"(id, date_at, name, description) VALUES ('1', '2020-01-21', 'Uruguay', null);";
            connection.createQuery(sql).executeUpdate();

            String sql2 = "INSERT INTO "+ escapeTableName("country")+"(id, date_at, name, description) VALUES ('2', '2020-02-22', 'UK', 'UK description');";
            connection.createQuery(sql2).executeUpdate();
        }

        DefaultApiRequest.loadDataRequestAndAssertion("/data",
                "suites/common/data/load/types/load-request.json",
                configurationParameters(),
                "suites/common/data/load/types/load-response.json",
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
