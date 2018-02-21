package com.exqudens.hibernate.repository;

import java.io.Serializable;
import java.util.List;

public interface Repository<T, ID extends Serializable> {

    <S extends T> List<S> save(List<S> entities);

    List<T> findAll();
    List<T> findAll(List<ID> ids);

    <S extends T> void update(List<S> entities);

    <S extends T> void delete(List<S> entities);

}
