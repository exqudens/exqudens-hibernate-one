package com.exqudens.hibernate.listener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.hibernate.HibernateException;
import org.hibernate.event.spi.DeleteEvent;
import org.hibernate.event.spi.DeleteEventListener;
import org.hibernate.event.spi.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteEventListenerImpl implements DeleteEventListener {

    private static final Logger LOG;
    private static final long serialVersionUID;

    static {
        LOG = LoggerFactory.getLogger(DeleteEventListenerImpl.class);
        LOG.trace("");
        serialVersionUID = -1802522419395160778L;
    }

    private final Map<String, List<Object>> cache;

    public DeleteEventListenerImpl() {
        super();
        LOG.trace("");
        cache = new ConcurrentHashMap<>();
    }

    @Override
    public void onDelete(DeleteEvent e) throws HibernateException {
        LOG.trace("");
        internalOnDelete(e);
    }

    @Override
    public void onDelete(DeleteEvent e, @SuppressWarnings("rawtypes") Set s) throws HibernateException {
        LOG.trace("");
        internalOnDelete(e);
    }

    public synchronized List<Object> remove(String sessionIdentifierExt) {
        LOG.trace("");
        return cache.remove(sessionIdentifierExt);
    }

    private synchronized void internalOnDelete(DeleteEvent e) {
        LOG.trace("");
        if (e.getSession() != null && e.getObject() != null) {
            LOG.trace("e.entity: {}", e.getObject());
            EventSource eventSource = e.getSession();
            Object entity = e.getObject();
            String sessionIdentifierExt = eventSource.getSessionIdentifier().toString() + System.identityHashCode(eventSource);
            cache.putIfAbsent(sessionIdentifierExt, new ArrayList<>());
            cache.get(sessionIdentifierExt).add(entity);
        }
    }

}
