package com.example.mysqlserverutilbak.mysql;

import com.example.mysqlserverutilbak.mysql.log.CoreMarker;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;

import static com.example.mysqlserverutilbak.mysql.DifferenceInfo.*;

@Data
@Slf4j
@Component
public class DiffInfoRegistry {

    @Autowired
    private ValidateConfig config;

    // key = DiffType, value = <DiffKey, differenceInfo>
    private Map<DiffType, Map<String, DifferenceInfo>> differences = Maps.newHashMap();
    private Map<DiffType, Map<String, DifferenceInfo>> coreDifferences = Maps.newHashMap();

    public void registerDiffInfo(DifferenceInfo differenceInfo) {

        String dbName = differenceInfo.getDbName();
        String tableName = differenceInfo.getTableName();
        DiffType diffType = differenceInfo.getDiffType();
        Map<String, Set<String>> coreTables = config.getCoreTables();

        log.info(new CoreMarker(), "[register difference info]: diffType = {}, dbName = {}, tableName = {}, differenceInfo = {}", diffType, dbName, tableName, differenceInfo);

        if (coreTables.keySet().contains(dbName) && coreTables.get(dbName).contains(tableName)) {
            coreDifferences.computeIfAbsent(diffType, key -> Maps.newHashMap()).put(buildKey(dbName, tableName), differenceInfo);
        }

        differences.computeIfAbsent(diffType, (key) -> Maps.newHashMap()).put(buildKey(dbName, tableName), differenceInfo);
    }

    // According to dbName + table, get diffInfo;   用于关注core表，更加直观
    public DifferenceInfo getDiffInfo(String dbName, String tableName) {
        DifferenceInfo diffInfo = differences.entrySet().stream().
                map(entry -> {
                    DifferenceInfo differenceInfo = null;
                    Map<String, DifferenceInfo> diffMap = entry.getValue();

                    if (entry.getKey() != DiffType.MISS_DATABASE) {
                        differenceInfo = diffMap.get(buildKey(dbName, tableName));
                    } else {
                        differenceInfo = diffMap.get(buildKey(dbName, DBMissInfo.DB_MISS_TABLE));
                    }

                    return differenceInfo;
                }).filter(value -> value != null).findFirst().orElse(null);

        return diffInfo;
    }
}
