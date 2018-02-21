package com.exqudens.hibernate.integrator;

import java.util.Arrays;
import java.util.List;

import org.hibernate.integrator.spi.Integrator;
import org.hibernate.jpa.boot.spi.IntegratorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegratorProviderImpl implements IntegratorProvider {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(IntegratorProviderImpl.class);
    }

    private final List<Integrator> integrators;

    public IntegratorProviderImpl() {
        super();
        LOG.trace("");
        this.integrators = Arrays.asList(IntegratorImpl.INSTANCE);
    }

    @Override
    public List<Integrator> getIntegrators() {
        LOG.trace("");
        return integrators;
    }

}
