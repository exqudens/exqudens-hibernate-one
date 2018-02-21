package com.exqudens.hibernate.integrator;

import org.hibernate.boot.Metadata;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.internal.DefaultDeleteEventListener;
import org.hibernate.event.service.spi.DuplicationStrategy;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.jpa.event.internal.core.HibernateEntityManagerEventListener;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exqudens.hibernate.listener.DeleteEventListenerImpl;

public class IntegratorImpl /*extends org.hibernate.jpa.event.spi.JpaIntegrator*/ implements Integrator {

    public static final IntegratorImpl INSTANCE;

    private static final Logger LOG;
    private static final DuplicationStrategy REPLACE_ORIGINAL_DUPLICATION_STRATEGY;

    static {
        LOG = LoggerFactory.getLogger(IntegratorImpl.class);
        INSTANCE = new IntegratorImpl();
        REPLACE_ORIGINAL_DUPLICATION_STRATEGY = new ReplaceOriginalDuplicationStrategy();
    }

    private Metadata metadata;

    private IntegratorImpl() {
        super();
        LOG.trace("");
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public void integrate(Metadata metadata, SessionFactoryImplementor sessionFactory, SessionFactoryServiceRegistry serviceRegistry) {
        LOG.trace("");
        this.metadata = metadata;
        final EventListenerRegistry eventListenerRegistry = serviceRegistry.getService( EventListenerRegistry.class );
        eventListenerRegistry.addDuplicationStrategy(REPLACE_ORIGINAL_DUPLICATION_STRATEGY);
        eventListenerRegistry.setListeners(EventType.DELETE, new DefaultDeleteEventListener());
        eventListenerRegistry.getEventListenerGroup(EventType.DELETE).prependListener(new DeleteEventListenerImpl());
    }

    @Override
    public void disintegrate(SessionFactoryImplementor sessionFactory, SessionFactoryServiceRegistry serviceRegistry) {
        LOG.trace("");
        this.metadata = null;
    }

    private static class ReplaceOriginalDuplicationStrategy implements DuplicationStrategy {
        @Override
        public boolean areMatch(Object listener, Object original) {
            return listener.getClass().equals( original.getClass() ) &&
                    HibernateEntityManagerEventListener.class.isInstance( original );
        }

        @Override
        public Action getAction() {
            return Action.REPLACE_ORIGINAL;
        }
    }

}
