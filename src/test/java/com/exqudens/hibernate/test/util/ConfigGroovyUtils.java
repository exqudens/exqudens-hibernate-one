package com.exqudens.hibernate.test.util;

import groovy.util.ConfigObject;
import groovy.util.ConfigSlurper;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigGroovyUtils {

    private static final Logger LOG;

    static {
        LOG = LoggerFactory.getLogger(ConfigGroovyUtils.class);
        LOG.trace("");
    }

    public static Map<String, Object> toMap(String groovyString) {
        LOG.trace("");
        ConfigSlurper configSlurper = new ConfigSlurper();
        ConfigObject configObject = configSlurper.parse(groovyString);

        Map<?, ?> rawMap = configObject.flatten();

        return rawMap.entrySet().stream()
        .map(e -> new SimpleEntry<String, Object>(e.getKey().toString(), e.getValue()))
        .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);
    }

    public static Map<String, Object> retrieveProperties(
            Map<String, Object> configGroovy,
            String prefix,
            String... ignorePropertyKeys
    ) {
        LOG.trace("");
        Set<String> ignorePropertyKeysSet = Stream.of(ignorePropertyKeys).collect(Collectors.toSet());

        Map<String, Object> properties = configGroovy.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .map(e -> new SimpleEntry<>(e.getKey().replace(prefix, ""), e.getValue()))
        .filter(e -> !ignorePropertyKeysSet.contains(e.getKey()))
        .filter(e -> e.getValue() != null)
        .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll);

        return properties;
    }

    private ConfigGroovyUtils() {
        super();
        LOG.trace("");
    }

}
