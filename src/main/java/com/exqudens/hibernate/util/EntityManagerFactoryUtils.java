package com.exqudens.hibernate.util;

import java.util.Map;
import java.util.stream.Collectors;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.sql.DataSource;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.spi.IntegratorProvider;
import org.hibernate.persister.internal.PersisterClassResolverInitiator;
import org.hibernate.service.StandardServiceInitiators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exqudens.hibernate.multitenancy.MultiTenantConnectionProviderImpl;

public class EntityManagerFactoryUtils {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(EntityManagerFactoryUtils.class);
        LOG.trace("");
    }

    public static EntityManagerFactory createEntityManagerFactory(Map<String, DataSource> dataSourceMap, Map<String, Object> properties, Class<?>... classes) {
        LOG.trace("");
        try {
            Object integratorProviderClassName = properties.get(EntityManagerFactoryBuilderImpl.INTEGRATOR_PROVIDER);
            if (integratorProviderClassName != null) {
                Class<?> integratorProviderClass = Class.forName(integratorProviderClassName.toString());
                IntegratorProvider integratorProvider = IntegratorProvider.class.cast(integratorProviderClass.newInstance());
                properties.put(
                        EntityManagerFactoryBuilderImpl.INTEGRATOR_PROVIDER,
                        integratorProvider
                );
            }

            Object multiTenantConnectionProviderClassName = properties.get(AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER);
            if (multiTenantConnectionProviderClassName != null) {
                if (MultiTenantConnectionProviderImpl.class.getName().equals(multiTenantConnectionProviderClassName.toString())) {
                    MultiTenantConnectionProviderImpl multiTenantConnectionProviderImpl = new MultiTenantConnectionProviderImpl(dataSourceMap);
                    properties.put(
                            AvailableSettings.MULTI_TENANT_CONNECTION_PROVIDER,
                            multiTenantConnectionProviderImpl
                    );
                    properties.put(
                            AvailableSettings.MULTI_TENANT_IDENTIFIER_RESOLVER,
                            multiTenantConnectionProviderImpl.getCurrentTenantIdentifierResolver()
                    );
                }
            }

            StandardServiceInitiators.LIST = StandardServiceInitiators.LIST.stream()
            .map(s -> s instanceof PersisterClassResolverInitiator ? com.exqudens.hibernate.persister.PersisterClassResolverInitiatorImpl.INSTANCE : s)
            .collect(Collectors.toList());

            PersistenceUnitInfo info = PersistenceUnitInfoUtils.createPersistenceUnitInfo(
                    "default",
                    PersistenceUnitInfoUtils.HIBERNATE_PERSISTENCE_PROVIDER_CLASS_NAME,
                    properties,
                    classes
            );
            EntityManagerFactory entityManagerFactory = createEntityManagerFactory(info);
            return entityManagerFactory;
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static EntityManagerFactory createEntityManagerFactory(
            PersistenceUnitInfo info
    ) {
        LOG.trace("");
        try {
            ClassLoader cl = EntityManagerFactoryUtils.class.getClassLoader();
            Object o = cl.loadClass(info.getPersistenceProviderClassName()).newInstance();
            PersistenceProvider persistenceProvider = PersistenceProvider.class.cast(o);
            EntityManagerFactory emf = persistenceProvider.createContainerEntityManagerFactory(info, info.getProperties());
            return emf;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private EntityManagerFactoryUtils() {
        super();
        LOG.trace("");
    }

}
