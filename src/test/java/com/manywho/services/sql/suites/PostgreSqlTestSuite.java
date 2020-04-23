package com.manywho.services.sql.suites;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.suites.common.controllers.data.*;
import com.manywho.services.sql.suites.common.controllers.data.LoadTest;
import com.manywho.services.sql.suites.common.controllers.data.SaveTest;
import com.manywho.services.sql.suites.common.controllers.describe.DescribeTest;
import com.manywho.services.sql.suites.mysql.data.LoadBooleanTest;
import com.manywho.services.sql.suites.postgresql.data.*;
import com.manywho.services.sql.suites.postgresql.data.DeleteTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        // common
        LoadTest.class,
        MultipleKeyTest.class,
        SaveTest.class,
        DescribeTest.class,
        LoadTypesWithFilter.class,
        LoadWithoutOrderBy.class, // not for SqlServer
        //postgresql
        LoadSpecialForPostgresTest.class,
        LoadBooleanTest.class,
        DateTimeTest.class,
        UuidTest.class,
        CapitalLetterTest.class,
        AutoIncrementTest.class,
        DeleteTest.class,
        com.manywho.services.sql.suites.postgresql.data.LoadTest.class,
        com.manywho.services.sql.suites.postgresql.data.DeleteTest.class,
        com.manywho.services.sql.suites.postgresql.data.SaveTest.class,
        com.manywho.services.sql.suites.postgresql.describe.DescribeTest.class

})
public class PostgreSqlTestSuite {
    @BeforeClass
    public static void setUp() {
        DbConfigurationTest.setPorperties("postgresql");
    }
}
