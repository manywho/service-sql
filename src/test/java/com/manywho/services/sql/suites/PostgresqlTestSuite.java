package com.manywho.services.sql.suites;

import com.manywho.services.sql.ConfigurationDB;
import com.manywho.services.sql.suites.common.controllers.data.DateTimeTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        DateTimeTest.class,
        CommonTestSuite.class
})
public class PostgreSqlTestSuite {
    @BeforeClass
    public static void setUp() {
        ConfigurationDB.setPorperties("postgresql");
    }
}
