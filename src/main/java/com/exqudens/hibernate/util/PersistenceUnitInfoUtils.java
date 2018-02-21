package com.exqudens.hibernate.util;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.persistence.SharedCacheMode;
import javax.persistence.ValidationMode;
import javax.persistence.spi.ClassTransformer;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.PersistenceUnitTransactionType;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersistenceUnitInfoUtils {

    public static String HIBERNATE_PERSISTENCE_PROVIDER_CLASS_NAME;
    public static String ECLIPSELINK_PERSISTENCE_PROVIDER_CLASS_NAME;
    public static String OPENJPA_PERSISTENCE_PROVIDER_CLASS_NAME;

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(PersistenceUnitInfoUtils.class);
        LOG.trace("");
        HIBERNATE_PERSISTENCE_PROVIDER_CLASS_NAME = "org.hibernate.jpa.HibernatePersistenceProvider";
        ECLIPSELINK_PERSISTENCE_PROVIDER_CLASS_NAME = "org.eclipse.persistence.jpa.PersistenceProvider";
        OPENJPA_PERSISTENCE_PROVIDER_CLASS_NAME = "org.apache.openjpa.persistence.PersistenceProviderImpl";
    }

    /*public static PersistenceUnitInfo createPersistenceUnitInfoSpring(DataSource dataSource, Class<?>... classes) {
        LOG.trace("");
        org.springframework.orm.jpa.persistenceunit.DefaultPersistenceUnitManager persistenceUnitManager;
        persistenceUnitManager = new org.springframework.orm.jpa.persistenceunit.DefaultPersistenceUnitManager();
        persistenceUnitManager.setDefaultDataSource(dataSource);
        persistenceUnitManager.setPackagesToScan(classes[0].getPackage().getName());
        persistenceUnitManager.afterPropertiesSet();
        PersistenceUnitInfo info = persistenceUnitManager.obtainDefaultPersistenceUnitInfo();
        return info;
    }*/

    public static PersistenceUnitInfo createPersistenceUnitInfo(
            String persistenceUnitName,
            String persistenceProviderClassName,
            Map<String, ?> properties,
            Class<?>... classes
    ) {
        LOG.trace("");
        return createPersistenceUnitInfo(
                persistenceUnitName,
                persistenceProviderClassName,
                null,
                null,
                PersistenceUnitTransactionType.RESOURCE_LOCAL,
                properties,
                classes
        );
    }

    public static PersistenceUnitInfo createPersistenceUnitInfo(
            String persistenceUnitName,
            String persistenceProviderClassName,
            DataSource nonJtaDataSource,
            DataSource jtaDataSource,
            PersistenceUnitTransactionType persistenceUnitTransactionType,
            Map<String, ?> properties,
            Class<?>... classes
    ) {
        LOG.trace("");
        return new PersistenceUnitInfo() {

            @Override
            public String getPersistenceUnitName() {
                return persistenceUnitName;
            }

            @Override
            public String getPersistenceProviderClassName() {
                return persistenceProviderClassName;
            }

            @Override
            public DataSource getNonJtaDataSource() {
                return nonJtaDataSource;
            }

            @Override
            public DataSource getJtaDataSource() {
                return jtaDataSource;
            }

            @Override
            public PersistenceUnitTransactionType getTransactionType() {
                return persistenceUnitTransactionType;
            }

            @Override
            public Properties getProperties() {
                Properties p = new Properties();
                p.putAll(properties);
                return p;
            }

            @Override
            public List<String> getManagedClassNames() {
                List<String> list = Arrays
                .asList(classes)
                .stream()
                .map(Class::getName)
                .collect(Collectors.toList());
                return list;
            }

            @Override
            public boolean excludeUnlistedClasses() {
                return true;
            }

            @Override
            public List<URL> getJarFileUrls() {
                List<URL> list;
                //list = Collections.list(this.getClass().getClassLoader().getResources(""));
                list = Collections.emptyList();
                return list;
            }

            @Override
            public List<String> getMappingFileNames() {
                return Collections.emptyList();
            }

            @Override
            public ValidationMode getValidationMode() {
                return null;
            }

            @Override
            public SharedCacheMode getSharedCacheMode() {
                return null;
            }

            @Override
            public String getPersistenceXMLSchemaVersion() {
                return null;
            }

            @Override
            public URL getPersistenceUnitRootUrl() {
                return null;
            }

            @Override
            public ClassLoader getClassLoader() {
                return null;
            }

            @Override
            public ClassLoader getNewTempClassLoader() {
                return null;
            }

            @Override
            public void addTransformer(ClassTransformer transformer) {
            }

        };
    }

    private PersistenceUnitInfoUtils() {
        super();
        LOG.trace("");
    }

}
