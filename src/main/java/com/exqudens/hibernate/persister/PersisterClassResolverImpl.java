package com.exqudens.hibernate.persister;

import org.hibernate.mapping.Collection;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.internal.StandardPersisterClassResolver;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersisterClassResolverImpl extends StandardPersisterClassResolver implements PersisterClassResolver {

    private static final long serialVersionUID = -2279093707964592356L;
    private static final Logger LOG = LoggerFactory.getLogger(PersisterClassResolverImpl.class);

    public PersisterClassResolverImpl() {
        super();
        LOG.trace("");
    }

    @Override
    public Class<? extends EntityPersister> singleTableEntityPersister() {
        LOG.trace("");
        return SingleTableEntityPersisterImpl.class;
    }

    @Override
    public Class<? extends EntityPersister> joinedSubclassEntityPersister() {
        LOG.trace("");
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends EntityPersister> unionSubclassEntityPersister() {
        LOG.trace("");
        throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends CollectionPersister> getCollectionPersisterClass(Collection metadata) {
        LOG.trace("");
        return metadata.isOneToMany() ? oneToManyPersister() : basicCollectionPersister();
    }

    private Class<OneToManyPersisterImpl> oneToManyPersister() {
        LOG.trace("");
        return OneToManyPersisterImpl.class;
    }

    private Class<BasicCollectionPersisterImpl> basicCollectionPersister() {
        LOG.trace("");
        return BasicCollectionPersisterImpl.class;
    }

}
