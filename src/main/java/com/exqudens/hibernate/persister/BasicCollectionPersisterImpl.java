package com.exqudens.hibernate.persister;

import org.hibernate.MappingException;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.spi.access.CollectionRegionAccessStrategy;
import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.BasicCollectionPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicCollectionPersisterImpl extends BasicCollectionPersister {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(BasicCollectionPersisterImpl.class);
    }

    public BasicCollectionPersisterImpl(
            Collection collectionBinding,
            CollectionRegionAccessStrategy cacheAccessStrategy,
            PersisterCreationContext creationContext
    ) throws MappingException, CacheException {
        super(collectionBinding, cacheAccessStrategy, creationContext);
        LOG.trace("");
    }

}
