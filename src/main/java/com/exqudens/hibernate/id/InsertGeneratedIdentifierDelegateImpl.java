package com.exqudens.hibernate.id;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGeneratorHelper;
import org.hibernate.id.insert.Binder;
import org.hibernate.id.insert.IdentifierGeneratingInsert;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exqudens.hibernate.persister.PostInsertIdentityPersister;

public class InsertGeneratedIdentifierDelegateImpl implements InsertGeneratedIdentifierDelegate {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(InsertGeneratedIdentifierDelegateImpl.class);
        LOG.trace("");
    }

    private final IdentityGeneratorImpl generator;
    private final PostInsertIdentityPersister persister;
    private final MySQLDialect dialect;
    private final Map<String, Queue<Serializable>> cache;

    public InsertGeneratedIdentifierDelegateImpl(
            IdentityGeneratorImpl generator, 
            PostInsertIdentityPersister persister, 
            MySQLDialect dialect
    ) {
        super();
        LOG.trace("");
        this.generator = generator;
        this.persister = persister;
        this.dialect = dialect;
        this.cache = new HashMap<>();
    }

    @Override
    public IdentifierGeneratingInsert prepareIdentifierGeneratingInsert() {
        LOG.trace("");
        IdentifierGeneratingInsert insert = new IdentifierGeneratingInsert(dialect);
        insert.addIdentityColumn(persister.getRootTableKeyColumnNames()[0]);
        return insert;
    }

    @Override
    public synchronized Serializable performInsert(String insertSQL, SharedSessionContractImplementor session, Binder binder) {
        LOG.trace("");
        Set<String> emptyQueueKeys = new HashSet<>();
        for (String sessionIdentifier : cache.keySet()) {
            if (cache.get(sessionIdentifier).isEmpty()) {
                emptyQueueKeys.add(sessionIdentifier);
            }
        }
        for (String sessionIdentifier : emptyQueueKeys) {
            cache.remove(sessionIdentifier);
        }
        String sessionIdentifierExt = session.getSessionIdentifier().toString() + System.identityHashCode(session);
        if (!cache.containsKey(sessionIdentifierExt) || cache.get(sessionIdentifierExt).isEmpty()) {
            Queue<Serializable> queue = createQueue(
                insertSQL,
                session,
                generator.remove(sessionIdentifierExt, getJdbcBatchSize(session))
            );
            LOG.debug("{}", queue);
            cache.putIfAbsent(sessionIdentifierExt, queue);
        }
        Serializable serializable = cache.get(sessionIdentifierExt).remove();
        return serializable;
    }

    private int getJdbcBatchSize(SharedSessionContractImplementor session) {
        return session.getJdbcBatchSize() != null
        ? session.getJdbcBatchSize()
        : session.getFactory().getSessionFactoryOptions().getJdbcBatchSize();
    }

    private Queue<Serializable> createQueue(
            String insertSQL,
            SharedSessionContractImplementor session,
            List<Object> objects
    ) {
        LOG.trace("");
        if (insertSQL == null || session == null || objects == null) {
            return null;
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = session
            .getJdbcCoordinator()
            .getLogicalConnection()
            .getPhysicalConnection()
            .prepareStatement(insertSQL, PreparedStatement.RETURN_GENERATED_KEYS);
            for (Object entity : objects) {

                persister.dehydrateIdentityInsert(entity, ps, session);
                session.getJdbcServices().getSqlStatementLogger().logStatement(insertSQL);

                ps.addBatch();
            }
            ps.executeBatch();
            rs = ps.getGeneratedKeys();
            Queue<Serializable> queue = new PriorityQueue<>();
            while (rs.next()) {
                Serializable serializable = IdentifierGeneratorHelper.get(
                        rs,
                        persister.getRootTableKeyColumnNames()[0],
                        persister.getIdentifierType(),
                        session.getJdbcServices().getJdbcEnvironment().getDialect()
                );
                queue.add(serializable);
            }
            return queue;
        } catch (RuntimeException e) {
            LOG.error(insertSQL, e);
            throw e;
        } catch (Exception e) {
            LOG.error(insertSQL, e);
            throw new RuntimeException(e);
        } finally {
            releaseStatement(session, ps, rs);
        }
    }

    private void releaseStatement(SharedSessionContractImplementor session, PreparedStatement ps, ResultSet rs) {
        LOG.trace("");
        session.getJdbcCoordinator().getLogicalConnection().getResourceRegistry().release(rs, ps);
        session.getJdbcCoordinator().afterStatementExecution();
    }

}
