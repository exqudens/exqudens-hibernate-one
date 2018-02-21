package com.exqudens.hibernate.persister;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.hibernate.HibernateException;
import org.hibernate.engine.spi.SharedSessionContractImplementor;

public interface PostInsertIdentityPersister extends org.hibernate.id.PostInsertIdentityPersister {

    int dehydrateIdentityInsert(
            Object entity,
            PreparedStatement ps,
            SharedSessionContractImplementor session
    ) throws SQLException, HibernateException;

}
