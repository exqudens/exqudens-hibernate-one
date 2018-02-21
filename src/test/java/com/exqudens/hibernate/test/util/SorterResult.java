package com.exqudens.hibernate.test.util;

import java.util.List;
import java.util.Map.Entry;

class SorterResult {

    private final List<Entry<Integer, Object>> first;
    private final List<Entry<Integer, Object>> second;

    SorterResult(List<Entry<Integer, Object>> first, List<Entry<Integer, Object>> second) {
        super();
        this.first = first;
        this.second = second;
    }

    List<Entry<Integer, Object>> getFirst() {
        return first;
    }

    List<Entry<Integer, Object>> getSecond() {
        return second;
    }

}
