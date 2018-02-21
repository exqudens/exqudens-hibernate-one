package com.exqudens.hibernate.persister;

import java.util.Map;

import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.persister.spi.PersisterClassResolver;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersisterClassResolverInitiatorImpl implements StandardServiceInitiator<PersisterClassResolver> {

    public static final PersisterClassResolverInitiatorImpl INSTANCE;

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(PersisterClassResolverInitiatorImpl.class);
        INSTANCE = new PersisterClassResolverInitiatorImpl();
    }

    private PersisterClassResolverInitiatorImpl() {
        super();
        LOG.trace("");
    }

    @Override
    public Class<PersisterClassResolver> getServiceInitiated() {
        LOG.trace("");
        return PersisterClassResolver.class;
    }

    @Override
    public PersisterClassResolver initiateService(
            @SuppressWarnings("rawtypes") Map configurationValues,
            ServiceRegistryImplementor registry
    ) {
        LOG.trace("");
        return new PersisterClassResolverImpl();
    }

}
