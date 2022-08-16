package com.example.mysqlserverutilbak.mysql.log;

import org.slf4j.Marker;
import org.springframework.stereotype.Component;

import java.util.Iterator;

/**
 * @Author qch
 * @Date 2022/8/16 10:56 上午
 */
@Component
public class CoreMarker implements Marker {
    public final static String CORE_MARKER = "core_marker";

    @Override
    public String getName() {
        return CORE_MARKER;
    }

    @Override
    public void add(Marker marker) {

    }

    @Override
    public boolean remove(Marker marker) {
        return false;
    }

    @Override
    public boolean hasChildren() {
        return false;
    }

    @Override
    public boolean hasReferences() {
        return false;
    }

    @Override
    public Iterator<Marker> iterator() {
        return null;
    }

    @Override
    public boolean contains(Marker marker) {
        return false;
    }

    @Override
    public boolean contains(String s) {
        return false;
    }
}
