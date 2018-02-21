package com.exqudens.hibernate.test.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.type.CollectionType;
import org.hibernate.type.EntityType;
import org.hibernate.type.ForeignKeyDirection;
import org.hibernate.type.OneToOneType;
import org.hibernate.type.Type;

public class InsertActionSorter {

    private List<BatchIdentifier> latestBatches;
    private Map<BatchIdentifier, List<AbstractEntityInsertAction>> actionBatches;

    public List<AbstractEntityInsertAction> sort(List<AbstractEntityInsertAction> insertions) {
        // optimize the hash size to eliminate a rehash.
        this.latestBatches = new ArrayList<>( );
        this.actionBatches = new HashMap<>();

        for ( AbstractEntityInsertAction action : insertions ) {
            BatchIdentifier batchIdentifier = new BatchIdentifier(
                    action.getEntityName(),
                    action.getSession().getFactory().getMetamodel().entityPersister( action.getEntityName() ).getRootEntityName()
            );

            int index = latestBatches.indexOf( batchIdentifier );

            if ( index != -1 )  {
                batchIdentifier = latestBatches.get( index );
            }
            else {
                latestBatches.add( batchIdentifier );
            }
            addParentChildEntityNames( action, batchIdentifier );
            addToBatch( batchIdentifier, action );
        }
        //insertions.clear();

        // Examine each entry in the batch list, sorting them based on parent/child associations.
        for ( int i = 0; i < latestBatches.size(); i++ ) {
            BatchIdentifier batchIdentifier = latestBatches.get( i );

            // Iterate previous batches and make sure that parent types are before children
            // Since the outer loop looks at each batch entry individually, we need to verify that any
            // prior batches in the list are not considered children (or have a parent) of the current
            // batch.  If so, we reordered them.
            for ( int j = i - 1; j >= 0; j-- ) {
                BatchIdentifier prevBatchIdentifier = latestBatches.get( j );
                if ( prevBatchIdentifier.hasAnyParentEntityNames( batchIdentifier ) ) {
                    latestBatches.remove( batchIdentifier );
                    latestBatches.add( j, batchIdentifier );
                }
            }

            // Iterate next batches and make sure that children types are after parents.
            // Since the outer loop looks at each batch entry individually and the prior loop will reorder
            // entries as well, we need to look and verify if the current batch is a child of the next
            // batch or if the current batch is seen as a parent or child of the next batch.
            for ( int j = i + 1; j < latestBatches.size(); j++ ) {
                BatchIdentifier nextBatchIdentifier = latestBatches.get( j );

                final boolean nextBatchHasChild = nextBatchIdentifier.hasAnyChildEntityNames( batchIdentifier );
                final boolean batchHasChild = batchIdentifier.hasAnyChildEntityNames( nextBatchIdentifier );
                final boolean batchHasParent = batchIdentifier.hasAnyParentEntityNames( nextBatchIdentifier );

                // Take care of unidirectional @OneToOne associations but exclude bidirectional @ManyToMany
                if ( ( nextBatchHasChild && !batchHasChild ) || batchHasParent ) {
                    latestBatches.remove( batchIdentifier );
                    latestBatches.add( j, batchIdentifier );
                }
            }
        }

        // now rebuild the insertions list. There is a batch for each entry in the name list.
        List<AbstractEntityInsertAction> sorted = new ArrayList<>();
        for ( BatchIdentifier rootIdentifier : latestBatches ) {
            List<AbstractEntityInsertAction> batch = actionBatches.get( rootIdentifier );
            sorted.addAll(sortBatch(batch));
        }
        return sorted;
    }

    private List<AbstractEntityInsertAction> sortBatch(List<AbstractEntityInsertAction> batch) {
        List<AbstractEntityInsertAction> first = new ArrayList<>();
        List<AbstractEntityInsertAction> second = new ArrayList<>();
        for (AbstractEntityInsertAction action : batch) {
            Object[] propertyValues = action.getState();
            ClassMetadata classMetadata = action.getPersister().getClassMetadata();
            boolean added = false;

            if ( classMetadata != null ) {
                Type[] propertyTypes = classMetadata.getPropertyTypes();

                for ( int i = 0; i < propertyValues.length; i++ ) {
                    Object value = propertyValues[i];
                    Type type = propertyTypes[i];
                    if ( type.isEntityType() && value != null ) {
                        second.add(action);
                        added = true;
                        break;
                    } else if ( type.isCollectionType() && value != null && !Collection.class.cast(value).isEmpty()) {
                        first.add(action);
                        added = true;
                        break;
                    }
                }
            }
            if (!added) {
                first.add(action);
            }
        }
        List<AbstractEntityInsertAction> sorted = new ArrayList<>();
        sorted.addAll(first);
        sorted.addAll(second);
        return sorted;
    }

    private void addParentChildEntityNames(AbstractEntityInsertAction action, BatchIdentifier batchIdentifier) {
        Object[] propertyValues = action.getState();
        ClassMetadata classMetadata = action.getPersister().getClassMetadata();
        if ( classMetadata != null ) {
            Type[] propertyTypes = classMetadata.getPropertyTypes();

            for ( int i = 0; i < propertyValues.length; i++ ) {
                Object value = propertyValues[i];
                Type type = propertyTypes[i];
                if ( type.isEntityType() && value != null ) {
                    EntityType entityType = (EntityType) type;
                    String entityName = entityType.getName();
                    String rootEntityName = action.getSession().getFactory().getMetamodel().entityPersister( entityName ).getRootEntityName();

                    if ( entityType.isOneToOne() &&
                            OneToOneType.class.cast( entityType ).getForeignKeyDirection() == ForeignKeyDirection.TO_PARENT ) {
                        batchIdentifier.getChildEntityNames().add( entityName );
                        if ( !rootEntityName.equals( entityName ) ) {
                            batchIdentifier.getChildEntityNames().add( rootEntityName );
                        }
                    }
                    else {
                        batchIdentifier.getParentEntityNames().add( entityName );
                        if ( !rootEntityName.equals( entityName ) ) {
                            batchIdentifier.getParentEntityNames().add( rootEntityName );
                        }
                    }
                }
                else if ( type.isCollectionType() && value != null ) {
                    CollectionType collectionType = (CollectionType) type;
                    final SessionFactoryImplementor sessionFactory = ( (SessionImplementor) action.getSession() )
                            .getSessionFactory();
                    if ( collectionType.getElementType( sessionFactory ).isEntityType() ) {
                        String entityName = collectionType.getAssociatedEntityName( sessionFactory );
                        String rootEntityName = action.getSession().getFactory().getMetamodel().entityPersister( entityName ).getRootEntityName();
                        batchIdentifier.getChildEntityNames().add( entityName );
                        if ( !rootEntityName.equals( entityName ) ) {
                            batchIdentifier.getChildEntityNames().add( rootEntityName );
                        }
                    }
                }
            }
        }
    }

    private void addToBatch(BatchIdentifier batchIdentifier, AbstractEntityInsertAction action) {
        List<AbstractEntityInsertAction> actions = actionBatches.get( batchIdentifier );

        if ( actions == null ) {
            actions = new LinkedList<>();
            actionBatches.put( batchIdentifier, actions );
        }
        actions.add( action );
    }

    public static class BatchIdentifier {

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
