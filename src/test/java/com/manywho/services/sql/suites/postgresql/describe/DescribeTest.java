package com.manywho.services.sql.suites.postgresql.describe;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.ServiceFunctionalTest;
import com.manywho.services.sql.utilities.DefaultApiRequest;
import org.junit.After;
import org.junit.Test;
import org.sql2o.Connection;

public class DescribeTest extends ServiceFunctionalTest {

    @Test
    public void testDescribeWithTypes() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");
        String sql = "CREATE TABLE " + escapeTableName("country2") + "(" +
                "id integer NOT NULL," +
                "name character varying(255)," +
                "description character varying(1024)," +
                "available BOOLEAN," +
                "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                ");";

        try (Connection connection = getSql2o().open()) {
            connection.createQuery(sql).executeUpdate();
        }

        DefaultApiRequest.describeServiceRequestAndAssertion("/metadata",
                "suites/postgresql/describe/with-types/metadata1-request.json",
                configurationParameters(),
                "suites/postgresql/describe/with-types/metadata1-response.json",
                dispatcher
        );
    }

    @Test
    public void testDescribeWithAliases() throws Exception {
        DbConfigurationTest.setPropertiesIfNotInitialized("postgresql");

        String sql = "CREATE TABLE " + escapeTableName("country") + "(" +
                "id integer NOT NULL," +
                "name character varying(255)," +
                "description character varying(1024)," +
                "CONSTRAINT country_id_pk PRIMARY KEY (id)" +
                ");";

        String aliasName = "COMMENT ON COLUMN " + escapeTableName("country") + ".name IS '{{ManyWhoName:The Name}}';";
        //ignored at the moment
        String aliasTable = "COMMENT ON TABLE " + escapeTableName("country") + " IS '{{ManyWhoName:The Country}}';";

        try (Connection connection = getSql2o().open()) {
            connection.createQuery(sql).executeUpdate();
            connection.createQuery(aliasName).executeUpdate();
            connection.createQuery(aliasTable).executeUpdate();
        }

        DefaultApiRequest.describeServiceRequestAndAssertion("/metadata",
                "suites/common/describe/with-alias-types/metadata1-request.json",
                configurationParameters(),
                "suites/common/describe/with-alias-types/metadata1-response.json",
                dispatcher
        );
    }

    @After
    public void cleanDatabaseAfterEachTest() {
        try (Connection connection = getSql2o().open()) {
            deleteTableIfExist("country", connection);
            deleteTableIfExist("country2", connection);
            deleteTableIfExist("timetest", connection);
        } catch (ClassNotFoundException e) {
        }
    }
}
