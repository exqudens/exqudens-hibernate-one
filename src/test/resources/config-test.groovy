dataSources {
    protocol    = "jdbc"
    subProtocol = "mysql"
    exqudensHibernateDataSource {
        host              = "localhost"
        port              = 3306
        dbName            = "exqudens_hibernate"
        jdbcUrlParams     = "?createDatabaseIfNotExist=true&rewriteBatchedStatements=true&serverTimezone=UTC&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&characterSetResults=UTF-8&logger=com.mysql.cj.core.log.Slf4JLogger&profileSQL=true"

        username          = "root"
        password          = null
        driverClassName   = "com.mysql.cj.jdbc.Driver"
        connectionTimeout = 40000
        readOnly          = false
        maximumPoolSize   = 1
        jdbcUrl           = protocol + ":" + subProtocol + "://" + host + ":" + port + "/" + dbName + jdbcUrlParams
    }
}

jpaProviders {
    hibernateJpaProvider {
        properties = [
            // 'hibernate.jdbc.batch.builder'                                  : "org.hibernate.engine.jdbc.batch.internal.BatchBuilderImpl",
            'hibernate.ejb.identifier_generator_strategy_provider'          : "com.exqudens.hibernate.id.IdentifierGeneratorStrategyProviderImpl",

            'hibernate.multiTenancy'                                        : "DATABASE",
            'hibernate.multi_tenant_connection_provider'                    : "com.exqudens.hibernate.multitenancy.MultiTenantConnectionProviderImpl",
            // 'hibernate.connection.handling_mode'                            : "DELAYED_ACQUISITION_AND_RELEASE_AFTER_STATEMENT",

            'hibernate.integrator_provider'                                 : "com.exqudens.hibernate.integrator.IntegratorProviderImpl",
            // 'hibernate.ejb.event.delete'                                    : "com.exqudens.hibernate.listener.DeleteEventListenerImpl",

            'hibernate.dialect'                                             : "org.hibernate.dialect.MySQL57Dialect",
            'hibernate.globally_quoted_identifiers'                         : true,
            'hibernate.globally_quoted_identifiers_skip_column_definitions' : true,
            'hibernate.dialect.storage_engine'                              : "innodb",
            'org.hibernate.flushMode'                                       : "COMMIT",
            'hibernate.default_batch_fetch_size'                            : 1000,
            'hibernate.jdbc.use_get_generated_keys'                         : true,
            'hibernate.transaction.flush_before_completion'                 : false,
            'hibernate.jdbc.fetch_size'                                     : 1000,
            'hibernate.jdbc.batch_size'                                     : 1000,
            'hibernate.batch_fetch_style'                                   : "DYNAMIC",
            'hibernate.order_inserts'                                       : true,
            'hibernate.order_updates'                                       : true,
            'hibernate.jdbc.batch_versioned_data'                           : true,
            'hibernate.hbm2ddl.auto'                                        : "create-drop",
            'hibernate.use_sql_comments'                                    : false,
            'hibernate.format_sql'                                          : false,
            'hibernate.show_sql'                                            : false
        ]
    }
}
