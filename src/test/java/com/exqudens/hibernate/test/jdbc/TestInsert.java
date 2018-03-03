package com.exqudens.hibernate.test.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.junit.Test;

import com.mysql.cj.jdbc.MysqlDataSource;

public class TestInsert {

    private static final DataSource DS;

    static {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setServerName("localhost");
        ds.setPort(3306);
        ds.setUser("root");
        ds.setDatabaseName("exqudens_hibernate");

        DS = ds;
    }

    @Test
    public void test() {
        try (Connection c = DS.getConnection()) {
            String sql;

            System.out.println("==============================");
            sql = Stream.of(
                "delete from `user`"
            ).collect(Collectors.joining());
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.executeUpdate();
            }

            System.out.println("==============================");
            sql = Stream.of(
                "insert into `user`(`user_id`, `email`) values",
                "(null, 'email_1'),",
                "(11, 'email_2'),",
                "(null, 'email_3')"
            ).collect(Collectors.joining());
            try (PreparedStatement ps = c.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    ResultSetMetaData md = rs.getMetaData();
                    while (rs.next()) {
                        System.out.println("------------------------------");
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            System.out.println(md.getColumnName(i) + "=" + rs.getObject(i).toString());
                        }
                    }
                }
            }

            System.out.println("==============================");
            sql = Stream.of(
                "select * from `user`"
            ).collect(Collectors.joining());
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                try (ResultSet rs = ps.executeQuery()) {
                    ResultSetMetaData md = rs.getMetaData();
                    while (rs.next()) {
                        System.out.println("------------------------------");
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            System.out.println(md.getColumnName(i) + "=" + rs.getObject(i).toString());
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
