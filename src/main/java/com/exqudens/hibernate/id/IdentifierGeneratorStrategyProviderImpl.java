package com.exqudens.hibernate.id;

import java.util.HashMap;
import java.util.Map;

import org.hibernate.jpa.spi.IdentifierGeneratorStrategyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentifierGeneratorStrategyProviderImpl implements IdentifierGeneratorStrategyProvider {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(IdentifierGeneratorStrategyProviderImpl.class);
        LOG.trace("");
    }

    private final Map<String, Class<?>> strategies;

    public IdentifierGeneratorStrategyProviderImpl() {
        super();
        LOG.trace("");
        strategies = new HashMap<>();
        strategies.put("identity", IdentityGeneratorImpl.class);
    }

    @Override
    public Map<String, Class<?>> getStrategies() {
        LOG.trace("");
        return strategies;
    }

}
