package com.exqudens.hibernate.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exqudens.hibernate.test.model.a.Item;
import com.exqudens.hibernate.test.model.a.Order;
import com.exqudens.hibernate.test.model.a.User;
import com.exqudens.hibernate.test.util.ClassPathUtils;
import com.exqudens.hibernate.test.util.ConfigGroovyUtils;
import com.exqudens.hibernate.test.util.DataSourceUtils;
import com.exqudens.hibernate.util.EntityManagerFactoryUtils;
import com.exqudens.hibernate.util.SortUtils;

public class TestADelete {

    private static final Logger LOG;
    private static final String DS_PREFIX;
    private static final String JPA_PREFIX;
    private static final String[] DS_IGNORE_KEYS;

    static {
        LOG = LoggerFactory.getLogger(TestADelete.class);
        LOG.trace("");
        DS_PREFIX = "dataSources.exqudensHibernateDataSource.";
        JPA_PREFIX = "jpaProviders.hibernateJpaProvider.properties.";
        DS_IGNORE_KEYS = new String[] {"host", "port", "dbName", "jdbcUrlParams"};
    }

    public TestADelete() {
        super();
        LOG.trace("");
    }

    @Test
    public void test() {
        LOG.trace("");
        EntityManagerFactory emf = null;
        EntityManager em = null;
        try {
            List<User> users = new ArrayList<>();
            List<Order> orders = new ArrayList<>();
            List<Item> items = new ArrayList<>();

            users.add(new User(null, null, "email_" + 1, new ArrayList<>()));

            orders.add(new Order(null, null, "orderNumber_" + 1, null, new ArrayList<>()));
            orders.add(new Order(null, null, "orderNumber_" + 2, null, new ArrayList<>()));
            orders.add(new Order(null, null, "orderNumber_" + 3, null, new ArrayList<>()));

            items.add(new Item(null, null, "description_" + 1, null, null, new ArrayList<>()));
            items.add(new Item(null, null, "description_" + 2, null, null, new ArrayList<>()));
            items.add(new Item(null, null, "description_" + 3, null, null, new ArrayList<>()));

            users.get(0).getOrders().addAll(orders);

            orders.stream().forEach(o -> o.setUser(users.get(0)));
            orders.get(1).setItems(items);

            items.stream().forEach(i -> i.setOrder(orders.get(1)));
            /*items.get(1).getChildren().add(items.get(0));
            items.get(1).getChildren().add(items.get(2));
            items.get(0).setParent(items.get(1));
            items.get(2).setParent(items.get(1));*/

            emf = createEntityManagerFactory(DS_PREFIX, JPA_PREFIX, DS_IGNORE_KEYS);

            em = emf.createEntityManager();
            em.persist(users.get(0));
            sortPrint(em);
            em.getTransaction().begin();
            em.flush();
            em.getTransaction().commit();
            em.clear();
            em.close();

            em = emf.createEntityManager();
            User user = em.find(User.class, 1L);
            em.remove(user);
            sortPrint(em);
            em.getTransaction().begin();
            em.flush();
            em.getTransaction().commit();
            em.clear();
            em.close();

        } catch (RuntimeException e) {
            em.getTransaction().rollback();
            LOG.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            em.getTransaction().rollback();
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if (em != null && em.isOpen()) {
                em.close();
            }
            if (emf != null && emf.isOpen()) {
                //emf.close();
            }
        }
    }

    private void sortPrint(EntityManager em) {
        SharedSessionContractImplementor session = SharedSessionContractImplementor.class.cast(em);
        List<Object> entries = Arrays
        .stream(session.getPersistenceContext().reentrantSafeEntityEntries())
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());
        System.out.println("===================================================================");
        for (Object entry : entries) {
            System.out.println(entry);
        }
        System.out.println("===================================================================");
        List<List<Object>> sort = SortUtils.sort(entries, session);
        for (List<Object> batch : sort) {
            System.out.println("-------------------------------------------------------------------");
            for (Object entry : batch) {
                System.out.println(entry);
            }
        }
    }

    private EntityManagerFactory createEntityManagerFactory(
            String dataSourcePrefix,
            String jpaPrefix,
            String... dataSourceIgnoreKeys
    ) {
        LOG.trace("");
        try {
            Map<String, Object> configMap = ConfigGroovyUtils.toMap(ClassPathUtils.toString("config-test.groovy"));

            DataSource dataSource = DataSourceUtils
            .createDataSource(
                ConfigGroovyUtils.retrieveProperties(configMap, dataSourcePrefix, dataSourceIgnoreKeys)
            );

            Map<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();
            dataSourceMap.put("any", dataSource);

            return EntityManagerFactoryUtils
            .createEntityManagerFactory(
                    dataSourceMap,
                    ConfigGroovyUtils.retrieveProperties(configMap, jpaPrefix),
                    User.class,
                    Order.class,
                    Item.class
            );
        } catch (RuntimeException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

}
