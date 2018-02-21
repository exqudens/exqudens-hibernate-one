package com.exqudens.hibernate.repository;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.hibernate.Session;
import org.hibernate.engine.jdbc.connections.spi.MultiTenantConnectionProvider;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exqudens.hibernate.multitenancy.MultiTenantConnectionProviderImpl;

public class HibernateRepository<T, ID extends Serializable> implements Repository<T, ID> {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(HibernateRepository.class);
    }

    private final Class<T> entityClass;
    private final Class<ID> idClass;
    private final EntityManager em;

    public HibernateRepository(Class<T> entityClass, Class<ID> idClass, EntityManager em) {
        super();
        LOG.trace("");
        this.entityClass = entityClass;
        this.idClass = idClass;
        this.em = em;
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    public Class<ID> getIdClass() {
        return idClass;
    }

    @Override
    public <S extends T> List<S> save(List<S> entities) {
        LOG.trace("");
        if (entities == null || entities.isEmpty()) {
            return entities;
        }
        for (S entity : entities) {
            em.persist(entity);
        }
        LOG.trace("");
        em.getTransaction().begin();
        em.flush();
        em.getTransaction().commit();
        em.clear();
        return entities;
    }

    @Override
    public List<T> findAll() {
        preQuery();
        return typedQueryFindAll().getResultList();
    }

    @Override
    public List<T> findAll(List<ID> ids) {
        LOG.trace("");
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        }
        preQuery();
        return typedQueryFindAll(ids).getResultList();
    }

    @Override
    public <S extends T> void update(List<S> entities) {
        LOG.trace("");
        if (entities == null || entities.isEmpty()) {
            return;
        }
        SharedSessionContractImplementor sessionContract = em.unwrap(SharedSessionContractImplementor.class);
        List<ID> ids = entities.stream()
        .map(entity -> sessionContract.getEntityPersister(null, entity).getIdentifier(entity, sessionContract))
        .map(id -> idClass.cast(id))
        .collect(Collectors.toList());
        List<T> loadedEntities = em.unwrap(Session.class).byMultipleIds(entityClass).multiLoad(ids);
        if (entities.size() != loadedEntities.size()) {
            throw new RuntimeException("entities.size != loadedEntities.size");
        }
        for (T entity : entities) {
            em.merge(entity);
        }
        LOG.trace("");
        em.getTransaction().begin();
        em.flush();
        em.getTransaction().commit();
        em.clear();
    }

    @Override
    public <S extends T> void delete(List<S> entities) {
        LOG.trace("");
        if (entities == null || entities.isEmpty()) {
            return;
        }
        /*SharedSessionContractImplementor sessionContract = em.unwrap(SharedSessionContractImplementor.class);
        List<ID> ids = entities.stream()
        .map(entity -> sessionContract.getEntityPersister(null, entity).getIdentifier(entity, sessionContract))
        .map(id -> idClass.cast(id))
        .collect(Collectors.toList());
        List<T> multiLoad = em.unwrap(Session.class).byMultipleIds(entityClass).multiLoad(ids);*/
        for (T entity : entities) {
            em.remove(entity);
            /*sessionContract
            .getEntityPersister(null, entity)
            .delete(sessionContract.getEntityPersister(null, entity).getIdentifier(entity, sessionContract), null, entity, sessionContract);*/
        }
        LOG.trace("");
        em.getTransaction().begin();
        em.flush();
        em.getTransaction().commit();
        em.clear();
    }

    private TypedQuery<T> typedQueryFindAll() {
        LOG.trace("");
        String jpql = Arrays.asList(
                "from ",
                entityClass.getSimpleName(),
                " order by id"
        ).stream().collect(Collectors.joining());
        TypedQuery<T> typedQuery = em.createQuery(jpql, entityClass)
        .setMaxResults(getJdbcBatchSize());
        return typedQuery;
    }

    private TypedQuery<T> typedQueryFindAll(List<ID> ids) {
        LOG.trace("");
        String jpql = Arrays.asList(
                "from ",
                entityClass.getSimpleName(),
                " where id in :ids"
        ).stream().collect(Collectors.joining());
        SharedSessionContractImplementor sessionContract = em.unwrap(SharedSessionContractImplementor.class);
        sessionContract.getJdbcBatchSize();
        TypedQuery<T> typedQuery = em.createQuery(jpql, entityClass)
        .setMaxResults(getJdbcBatchSize())
        .setParameter("ids", ids);
        return typedQuery;
    }

    private int getJdbcBatchSize() {
        SharedSessionContractImplementor session = em.unwrap(SharedSessionContractImplementor.class);
        return session.getJdbcBatchSize() != null
        ? session.getJdbcBatchSize()
        : session.getFactory().getSessionFactoryOptions().getJdbcBatchSize();
    }

    private void preQuery() {
        SharedSessionContractImplementor session = em.unwrap(SharedSessionContractImplementor.class);
        PhysicalConnectionHandlingMode mode1 = PhysicalConnectionHandlingMode.DELAYED_ACQUISITION_AND_RELEASE_AFTER_STATEMENT;
        PhysicalConnectionHandlingMode mode2 = session.getJdbcCoordinator().getLogicalConnection().getConnectionHandlingMode();
        if (mode1.equals(mode2)) {
            LOG.trace("");
            MultiTenantConnectionProvider service = session.getFactory().getServiceRegistry().getService(MultiTenantConnectionProvider.class);
            MultiTenantConnectionProviderImpl.class.cast(service).setDataSourceKey(entityClass.getName());
            session.getJdbcCoordinator().getLogicalConnection().manualDisconnect();
        }
    }

}
