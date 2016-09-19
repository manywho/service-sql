package com.manywho.services.sql;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.manywho.sdk.services.jaxrs.resolvers.ObjectMapperContextResolver;
import org.jboss.resteasy.core.Dispatcher;
import org.jboss.resteasy.mock.MockDispatcherFactory;
import org.junit.BeforeClass;
import org.sql2o.Connection;
import org.sql2o.Sql2o;

import javax.ws.rs.Path;
import java.util.HashMap;
import java.util.Objects;

public abstract class ServiceFunctionalTest {
    private Sql2o sql2o;
    protected static Dispatcher dispatcher;
    protected static ObjectMapper objectMapper;

    @BeforeClass
    public static void setUp() {
        TestApplication application = new TestApplication();
        application.addModule(new ApplicationSqlModule());
        application.initialize("com.manywho.services.sql");


        objectMapper = new ObjectMapperContextResolver().getContext(null);
        dispatcher = MockDispatcherFactory.createDispatcher();

        for (Class<?> klass : application.getClasses()) {
            dispatcher.getRegistry().addPerRequestResource(klass);
        }

        dispatcher.getProviderFactory().registerProvider(ObjectMapperContextResolver.class);

        for (Object singleton : application.getSingletons()) {
            if (singleton.getClass().isAnnotationPresent(Path.class)) {
                dispatcher.getRegistry().addSingletonResource(singleton);
            } else if (singleton.getClass().getSuperclass().isAnnotationPresent(Path.class)) {
                dispatcher.getRegistry().addSingletonResource(singleton);
            }
        }

    }

    protected Sql2o getSql2o() throws ClassNotFoundException {
        if(sql2o != null) {

            return sql2o;
        } else {
            if(Objects.equals(DbConfigurationTest.databaseTypeForTest, "postgresql")) {
                Class.forName("com.mysql.jdbc.Driver");
                sql2o = new Sql2o("jdbc:postgresql://" + DbConfigurationTest.hostForTest+":" + DbConfigurationTest.portForTest + "/" + DbConfigurationTest.databaseNameForTest, "postgres", "admin");

            }else if(Objects.equals(DbConfigurationTest.databaseTypeForTest, "sqlserver")) {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                sql2o = new Sql2o("jdbc:sqlserver://" + DbConfigurationTest.hostForTest + ":" + DbConfigurationTest.portForTest + ";databaseName=" + DbConfigurationTest.databaseNameForTest, "postgres", "admin");

            }else if(Objects.equals(DbConfigurationTest.databaseTypeForTest, "mysql")) {
                Class.forName("com.mysql.jdbc.Driver");
                sql2o = new Sql2o("jdbc:mysql://" + DbConfigurationTest.hostForTest + ":" + DbConfigurationTest.portForTest + "/" + DbConfigurationTest.databaseNameForTest, "postgres", "admin");
            }

            return sql2o;
        }
    }

    protected HashMap<String, String> configurationParameters() {
        HashMap<String, String> replacements = new HashMap<>();
        replacements.put("{{port}}", DbConfigurationTest.portForTest);
        replacements.put("{{databaseType}}", DbConfigurationTest.databaseTypeForTest);
        replacements.put("{{schema}}", DbConfigurationTest.schemaForTest);
        replacements.put("{{host}}", DbConfigurationTest.hostForTest);

        return  replacements;
    }

    protected void deleteTableIfExist(String tableName, Connection connection){
        try {
            connection.createQuery("DROP TABLE "+ scapeTableName(tableName)).executeUpdate();
        } catch (Exception ex){
        }
    }

    public String scapeTableName( String tableName) {
        String format = "%s.%s";

        if(Objects.equals(DbConfigurationTest.databaseTypeForTest, "mysql")){
            format = "`%s`.`%s`";
        }

        return String.format(format, DbConfigurationTest.schemaForTest, tableName);
    }
}