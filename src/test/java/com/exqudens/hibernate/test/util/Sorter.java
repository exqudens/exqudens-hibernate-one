package com.exqudens.hibernate.test.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.Type;

class Sorter {

    SorterResult sort(List<Object> entities, SharedSessionContractImplementor session) {
        List<Entry<Integer, Object>> first = new ArrayList<>();
        List<Entry<Integer, Object>> second = new ArrayList<>();
        for (int i = 0; i < entities.size(); i++) {
            Object entity = entities.get(i);
            EntityPersister entityPersister = session.getEntityPersister(null, entity);
            Object[] propertyValues = entityPersister.getPropertyValuesToInsert(entity, Collections.emptyMap(), session);
            ClassMetadata classMetadata = entityPersister.getClassMetadata();
            boolean added = false;

            if ( classMetadata != null ) {
                Type[] propertyTypes = classMetadata.getPropertyTypes();

                for ( int j = 0; j < propertyValues.length; j++ ) {
                    Object value = propertyValues[j];
                    Type type = propertyTypes[j];
                    if ( type.isEntityType() && value != null ) {
                        second.add(new SimpleEntry<>(i, entity));
                        added = true;
                        break;
                    } else if ( type.isCollectionType() && value != null && !Collection.class.cast(value).isEmpty()) {
                        first.add(new SimpleEntry<>(i, entity));
                        added = true;
                        break;
                    }
                }
            }
            if (!added) {
                first.add(new SimpleEntry<>(i, entity));
            }
        }
        return new SorterResult(first, second);
    }

}
