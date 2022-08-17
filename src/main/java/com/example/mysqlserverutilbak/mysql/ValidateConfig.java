package com.example.mysqlserverutilbak.mysql;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
public class ValidateConfig {
    private int corePoolSize;
    private int maximumPoolSize;
    private long keepAliveTime;
    private Map<String, Set<String>> coreTables = Maps.newHashMap();
    private Map<String,Set<String>> excludeDBAndTable = Maps.newHashMap();
    private Set<String> excludeDataBase = Sets.newHashSet();
}
