package com.exqudens.hibernate.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.CollectionType;
import org.hibernate.type.EntityType;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortUtils {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(SortUtils.class);
        LOG.trace("");
    }

    private SortUtils() {
        super();
        LOG.trace("");
    }

    public static List<List<Object>> sort(List<Object> entities, SharedSessionContractImplementor session) {
        LOG.trace("");
        Map<BatchIdentifier, List<Object>> actionBatches = null;
        List<BatchIdentifier> latestBatches = new ArrayList<>();
        for (Object entity : entities) {
            EntityPersister entityPersister = session.getEntityPersister(null, entity);
            BatchIdentifier batchIdentifier = new BatchIdentifier(entityPersister.getEntityName(), entityPersister.getRootEntityName());

            int index = latestBatches.indexOf(batchIdentifier);
            if (index != -1) {
                batchIdentifier = latestBatches.get(index);
            } else {
                latestBatches.add(batchIdentifier);
            }
            addParentChildEntityNames(entity, batchIdentifier, session);
            actionBatches = addToBatch(batchIdentifier, entity, actionBatches);
        }

        for (int i = 0; i < latestBatches.size(); i++) {
            BatchIdentifier batchIdentifier = latestBatches.get(i);

            // Iterate previous batches and make sure that parent types are
            // before children
            // Since the outer loop looks at each batch entry individually, we
            // need to verify that any
            // prior batches in the list are not considered children (or have a
            // parent) of the current
            // batch. If so, we reordered them.
            for (int j = i - 1; j >= 0; j--) {
                BatchIdentifier prevBatchIdentifier = latestBatches.get(j);
                if (prevBatchIdentifier.hasAnyParentEntityNames(batchIdentifier)) {
                    latestBatches.remove(batchIdentifier);
                    latestBatches.add(j, batchIdentifier);
                }
            }

            // Iterate next batches and make sure that children types are after
            // parents.
            // Since the outer loop looks at each batch entry individually and
            // the prior loop will reorder
            // entries as well, we need to look and verify if the current batch
            // is a child of the next
            // batch or if the current batch is seen as a parent or child of the
            // next batch.
            for (int j = i + 1; j < latestBatches.size(); j++) {
                BatchIdentifier nextBatchIdentifier = latestBatches.get(j);

                final boolean nextBatchHasChild = nextBatchIdentifier.hasAnyChildEntityNames(batchIdentifier);
                final boolean batchHasChild = batchIdentifier.hasAnyChildEntityNames(nextBatchIdentifier);
                final boolean batchHasParent = batchIdentifier.hasAnyParentEntityNames(nextBatchIdentifier);

                // Take care of unidirectional @OneToOne associations but
                // exclude bidirectional @ManyToMany
                if ((nextBatchHasChild && !batchHasChild) || batchHasParent) {
                    latestBatches.remove(batchIdentifier);
                    latestBatches.add(j, batchIdentifier);
                }
            }
        }

        // now rebuild the insertions list. There is a batch for each entry in
        // the name list.
        List<List<Object>> sorted = new ArrayList<>();
        for (BatchIdentifier rootIdentifier : latestBatches) {
            List<Object> batch = actionBatches.get(rootIdentifier);
            List<List<Object>> sortedBatch = sortBatch(batch, session);
            for (List<Object> l : sortedBatch) {
                if (!l.isEmpty()) {
                    sorted.add(l);
                }
            }
        }
        return sorted;
    }

    private static void addParentChildEntityNames(Object entity, BatchIdentifier batchIdentifier, SharedSessionContractImplementor session) {
        LOG.trace("");
        EntityPersister entityPersister = session.getEntityPersister(null, entity);
        Object[] propertyValues = entityPersister.getPropertyValuesToInsert(entity, Collections.emptyMap(), session);
        ClassMetadata classMetadata = entityPersister.getClassMetadata();

        if (classMetadata != null) {
            Type[] propertyTypes = classMetadata.getPropertyTypes();

            for (int i = 0; i < propertyValues.length; i++) {
                Object value = propertyValues[i];
                Type type = propertyTypes[i];

                if (type.isEntityType() && value != null) {
                    EntityType entityType = (EntityType) type;
                    String entityName = entityType.getName();
                    String rootEntityName = session.getFactory().getMetamodel().entityPersister(entityName).getRootEntityName();

                    if (entityType.isOneToOne() && OneToOneType.class.cast(entityType).getForeignKeyDirection() == ForeignKeyDirection.TO_PARENT) {
                        batchIdentifier.getChildEntityNames().add(entityName);
                        if (!rootEntityName.equals(entityName)) {
                            batchIdentifier.getChildEntityNames().add(rootEntityName);
                        }
                    } else {
                        batchIdentifier.getParentEntityNames().add(entityName);
                        if (!rootEntityName.equals(entityName)) {
                            batchIdentifier.getParentEntityNames().add(rootEntityName);
                        }
                    }
                } else if (type.isCollectionType() && value != null) {
                    CollectionType collectionType = (CollectionType) type;
                    SessionFactoryImplementor sessionFactory = session.getFactory();
                    if (collectionType.getElementType(sessionFactory).isEntityType()) {
                        String entityName = collectionType.getAssociatedEntityName(sessionFactory);
                        String rootEntityName = session.getFactory().getMetamodel().entityPersister(entityName).getRootEntityName();
                        batchIdentifier.getChildEntityNames().add(entityName);
                        if (!rootEntityName.equals(entityName)) {
                            batchIdentifier.getChildEntityNames().add(rootEntityName);
                        }
                    }
                }
            }
        }
    }

    private static Map<BatchIdentifier, List<Object>> addToBatch(BatchIdentifier batchIdentifier, Object entity, Map<BatchIdentifier, List<Object>> actionBatches) {
        LOG.trace("");
        if (actionBatches == null) {
            actionBatches = new HashMap<>();
        }
        List<Object> entities = actionBatches.get(batchIdentifier);
        if (entities == null) {
            entities = new LinkedList<>();
            actionBatches.put(batchIdentifier, entities);
        }
        entities.add(entity);
        return actionBatches;
    }

    private static List<List<Object>> sortBatch(List<Object> entities, SharedSessionContractImplementor session) {
        LOG.trace("");
        List<Object> first = new ArrayList<>();
        List<Object> second = new ArrayList<>();
        for (Object entity : entities) {
            EntityPersister entityPersister = session.getEntityPersister(null, entity);
            Object[] propertyValues = entityPersister.getPropertyValuesToInsert(entity, Collections.emptyMap(), session);
            ClassMetadata classMetadata = entityPersister.getClassMetadata();
            boolean added = false;

            if (classMetadata != null) {
                Type[] propertyTypes = classMetadata.getPropertyTypes();

                for (int i = 0; i < propertyValues.length; i++) {
                    Object value = propertyValues[i];
                    Type type = propertyTypes[i];

                    if (type.isEntityType() && value != null) {
                        second.add(entity);
                        added = true;
                        break;
                    } else if (type.isCollectionType() && value != null && !Collection.class.cast(value).isEmpty()) {
                        first.add(entity);
                        added = true;
                        break;
                    }
                }
            }
            if (!added) {
                first.add(entity);
            }
        }
        List<List<Object>> sorted = new ArrayList<>();
        sorted.add(first);
        sorted.add(second);
        return sorted;
    }

    private static class BatchIdentifier {

        private final String entityName;
        private final String rootEntityName;

        private Set<String> parentEntityNames = new HashSet<>();
        private Set<String> childEntityNames = new HashSet<>();

        public BatchIdentifier(String entityName, String rootEntityName) {
            super();
            this.entityName = entityName;
            this.rootEntityName = rootEntityName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BatchIdentifier)) {
                return false;
            }
            BatchIdentifier that = (BatchIdentifier) o;
            return Objects.equals(entityName, that.entityName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(entityName);
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("BatchIdentifier [entityName=");
            builder.append(entityName);
            builder.append(", rootEntityName=");
            builder.append(rootEntityName);
            builder.append("]");
            return builder.toString();
        }

        public String getEntityName() {
            return entityName;
        }

        public String getRootEntityName() {
            return rootEntityName;
        }

        public Set<String> getParentEntityNames() {
            return parentEntityNames;
        }

        public Set<String> getChildEntityNames() {
            return childEntityNames;
        }

        public boolean hasAnyParentEntityNames(BatchIdentifier batchIdentifier) {
            return parentEntityNames.contains(batchIdentifier.getEntityName()) || parentEntityNames.contains(batchIdentifier.getRootEntityName());
        }

        public boolean hasAnyChildEntityNames(BatchIdentifier batchIdentifier) {
            return childEntityNames.contains(batchIdentifier.getEntityName());
        }

    }

}
