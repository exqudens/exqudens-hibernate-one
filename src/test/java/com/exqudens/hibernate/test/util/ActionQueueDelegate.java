package com.exqudens.hibernate.test.util;

import java.lang.reflect.Field;
import java.util.Arrays;

import org.hibernate.action.internal.AbstractEntityInsertAction;
import org.hibernate.engine.spi.ActionQueue;
import org.hibernate.engine.spi.ExecutableList;

public class ActionQueueDelegate {

    private final ActionQueue actionQueue;

    public ActionQueueDelegate(ActionQueue actionQueue) {
        super();
        this.actionQueue = actionQueue;
    }

    public ExecutableList<AbstractEntityInsertAction> getInsertions() {
        try {
            Field field = Arrays
            .stream(ActionQueue.class.getDeclaredFields())
            .filter(f -> "insertions".equals(f.getName()))
            .findFirst()
            .orElseThrow(null);
            field.setAccessible(true);
            Object object = field.get(actionQueue);
            Iterable<?> iterable = Iterable.class.cast(object);
            ExecutableList<AbstractEntityInsertAction> insertions = new ExecutableList<AbstractEntityInsertAction>(1, null);
            for (Object o : iterable) {
                AbstractEntityInsertAction action = AbstractEntityInsertAction.class.cast(o);
                insertions.add(action);
            }
            return insertions;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
