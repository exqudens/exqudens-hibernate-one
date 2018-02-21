package com.exqudens.hibernate.test.util;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSourceUtils {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(DataSourceUtils.class);
        LOG.trace("");
    }

    public static DataSource createDataSource(Map<String, Object> properties) {
        LOG.trace("");
        Properties p = new Properties();
        p.putAll(properties);
        HikariConfig hikariConfig = new HikariConfig(p);
        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        return dataSource;
    }

    public static void close(DataSource dataSource) {
        LOG.trace("");
        try {
            if (dataSource instanceof Closeable) {
                Closeable.class.cast(dataSource).close();
            } else if (dataSource instanceof AutoCloseable) {
                AutoCloseable.class.cast(dataSource).close();
            }
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private DataSourceUtils() {
        super();
        LOG.trace("");
    }

}
