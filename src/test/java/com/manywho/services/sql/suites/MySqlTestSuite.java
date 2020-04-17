package com.manywho.services.sql.suites;

import com.manywho.services.sql.DbConfigurationTest;
import com.manywho.services.sql.suites.common.controllers.data.*;
import com.manywho.services.sql.suites.common.controllers.describe.DescribeTest;
import com.manywho.services.sql.suites.mysql.data.AutoIncrementTest;
import com.manywho.services.sql.suites.mysql.data.CapitalLetterTest;
import com.manywho.services.sql.suites.mysql.data.LoadBooleanTest;
import com.manywho.services.sql.suites.mysql.describe.DescribeViewTest;
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
        // mysql
        CapitalLetterTest.class,
        AutoIncrementTest.class,
        LoadBooleanTest.class,
        com.manywho.services.sql.suites.mysql.describe.DescribeTest.class,
        com.manywho.services.sql.suites.mysql.data.SaveTest.class,
        DescribeViewTest.class
})
public class MySqlTestSuite {
    @BeforeClass
    public static void setUp() {
        DbConfigurationTest.setPorperties("mysql");
    }
}
