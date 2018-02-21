package com.exqudens.hibernate.multitenancy;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.engine.jdbc.connections.spi.MultiTenantConnectionProvider;
import org.hibernate.service.UnknownUnwrapTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTenantConnectionProviderImpl implements MultiTenantConnectionProvider {

    private static final Logger LOG;
    private static final long serialVersionUID;

    static {
        LOG = LoggerFactory.getLogger(MultiTenantConnectionProviderImpl.class);
        LOG.trace("");
        serialVersionUID = -6011064288935442704L;
    }

    private final Map<String, DataSource> dataSourceMap;
    private final CurrentTenantIdentifierResolver currentTenantIdentifierResolver;

    private String dataSourceKey;

    public MultiTenantConnectionProviderImpl(Map<String, DataSource> dataSourceMap) {
        super();
        LOG.trace("");
        this.dataSourceMap = dataSourceMap;
        this.dataSourceKey = dataSourceMap.entrySet().iterator().next().getKey();
        this.currentTenantIdentifierResolver = new CurrentTenantIdentifierResolver() {

            @Override
            public boolean validateExistingCurrentSessions() {
                LOG.trace("");
                return true;
            }

            @Override
            public String resolveCurrentTenantIdentifier() {
                LOG.trace("");
                return dataSourceKey;
            }
        };
    }

    public CurrentTenantIdentifierResolver getCurrentTenantIdentifierResolver() {
        return currentTenantIdentifierResolver;
    }

    public void setDataSourceKey(String dataSourceKey) {
        this.dataSourceKey = dataSourceKey;
    }

    @Override
    public Connection getAnyConnection() throws SQLException {
        LOG.trace("");
        return dataSourceMap.get(dataSourceKey).getConnection();
    }

    @Override
    public void releaseAnyConnection(Connection connection) throws SQLException {
        LOG.trace("");
        connection.close();
    }

    @Override
    public Connection getConnection(String tenantIdentifier) throws SQLException {
        LOG.trace("");
        if (dataSourceKey != null) {
            return dataSourceMap.get(dataSourceKey).getConnection();
        }
        return dataSourceMap.get(tenantIdentifier).getConnection();
    }

    @Override
    public void releaseConnection(String tenantIdentifier, Connection connection) throws SQLException {
        LOG.trace("");
        connection.close();
    }

    @Override
    public boolean supportsAggressiveRelease() {
        LOG.trace("");
        return true;
    }

    @Override
    public boolean isUnwrappableAs(@SuppressWarnings("rawtypes") Class unwrapType) {
        LOG.trace("");
        return MultiTenantConnectionProvider.class.equals(unwrapType) || getClass().equals(unwrapType);
    }

    @Override
    public <T> T unwrap(Class<T> unwrapType) {
        LOG.trace("");
        if (isUnwrappableAs(unwrapType)) {
            return unwrapType.cast(this);
        } else {
            throw new UnknownUnwrapTypeException( unwrapType );
        }
    }

}
