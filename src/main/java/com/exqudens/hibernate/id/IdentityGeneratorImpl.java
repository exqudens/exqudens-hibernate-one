package com.exqudens.hibernate.id;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.hibernate.HibernateException;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentityGenerator;
import org.hibernate.id.PostInsertIdentityPersister;
import org.hibernate.id.insert.InsertGeneratedIdentifierDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityGeneratorImpl extends IdentityGenerator {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(IdentityGeneratorImpl.class);
        LOG.trace("");
    }

    private final Map<String, List<Object>> cache;

    public IdentityGeneratorImpl() {
        super();
        LOG.trace("");
        this.cache = new HashMap<>();
    }

    @Override
    public synchronized Serializable generate(SharedSessionContractImplementor session, Object object) throws HibernateException {
        LOG.trace("");
        String sessionIdentifierExt = session.getSessionIdentifier().toString() + System.identityHashCode(session);
        cache.putIfAbsent(sessionIdentifierExt, new LinkedList<>());
        cache.get(sessionIdentifierExt).add(object);
        return super.generate(session, object);
    }

    @Override
    public InsertGeneratedIdentifierDelegate getInsertGeneratedIdentifierDelegate(
            PostInsertIdentityPersister persister,
            Dialect dialect,
            boolean isGetGeneratedKeysEnabled
    ) throws HibernateException {
        if (
                isGetGeneratedKeysEnabled
                && dialect instanceof MySQLDialect
                && persister instanceof com.exqudens.hibernate.persister.PostInsertIdentityPersister
        ) {
            return new InsertGeneratedIdentifierDelegateImpl(
                    this,
                    com.exqudens.hibernate.persister.PostInsertIdentityPersister.class.cast(persister),
                    MySQLDialect.class.cast(dialect)
            );
        }
        return super.getInsertGeneratedIdentifierDelegate(persister, dialect, isGetGeneratedKeysEnabled);
    }

    synchronized List<Object> remove(String sessionIdentifier, int jdbcBatchSize) {
        List<Object> all = cache.get(sessionIdentifier);
        List<List<Object>> partition = partition(all, jdbcBatchSize);
        if (partition.size() > 1) {
            List<Object> first = partition.remove(0);
            List<Object> other = partition.stream().flatMap(List::stream).collect(Collectors.toList());
            cache.put(sessionIdentifier, other);
            return first;
        } else {
            cache.remove(sessionIdentifier);
            return partition.get(0);
        }
    }

    private <E> List<List<E>> partition(List<E> list, int size) {
        Function<Entry<Integer, E>, Integer> classifier;
        classifier = (Entry<Integer, E> e) -> {
            int number = e.getKey().intValue();
            if (number % size != 0) {
                return number / size * size + size;
            } else {
                return number;
            }
        };
        Function<Entry<Integer, E>, E> mapper = (Entry<Integer, E> e) -> e.getValue();
        return IntStream.range(0, list.size())
        .mapToObj(i -> new SimpleEntry<>(i + 1, list.get(i)))
        .collect(
            Collectors.groupingBy(
                classifier,
                LinkedHashMap::new,
                Collectors.mapping(mapper, Collectors.toList())
            )
        ).values().stream()
        .collect(Collectors.toList());
    }

}
