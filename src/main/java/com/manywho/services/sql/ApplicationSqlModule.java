package com.manywho.services.sql;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.manywho.sdk.services.types.TypeProvider;
import com.manywho.services.sql.services.QueryParameterService;
import com.manywho.services.sql.types.RawTypeProvider;

public class ApplicationSqlModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(QueryParameterService.class).in(Singleton.class);
        bind(TypeProvider.class).to(RawTypeProvider.class);
    }
}
