package com.exqudens.hibernate.persister;

import java.io.Serializable;

import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.hibernate.engine.jdbc.connections.spi.MultiTenantConnectionProvider;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.OneToManyPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exqudens.hibernate.multitenancy.MultiTenantConnectionProviderImpl;

public class OneToManyPersisterImpl extends OneToManyPersister {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(OneToManyPersisterImpl.class);
    }

    public OneToManyPersisterImpl(
            Collection collectionBinding,
            CollectionRegionAccessStrategy cacheAccessStrategy,
            PersisterCreationContext creationContext
    ) throws MappingException, CacheException {
        super(collectionBinding, cacheAccessStrategy, creationContext);
        LOG.trace("");
    }

    @Override
    public void initialize(Serializable key, SharedSessionContractImplementor session) throws HibernateException {
        LOG.trace("");
        preInitialize(session);
        super.initialize(key, session);
    }

    private void preInitialize(SharedSessionContractImplementor session) {
        LOG.trace("");
        PhysicalConnectionHandlingMode mode1 = PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_RELEASE_AFTER_STATEMENT;
        PhysicalConnectionHandlingMode mode2 = session.getJdbcCoordinator().getLogicalConnection().getConnectionHandlingMode();
        if (mode1.equals(mode2)) {
            MultiTenantConnectionProvider service = session.getFactory().getServiceRegistry().getService(MultiTenantConnectionProvider.class);
            MultiTenantConnectionProviderImpl.class.cast(service).setDataSourceKey(getCollectionMetadata().getElementType().getReturnedClass().getName());
        }
    }

}
