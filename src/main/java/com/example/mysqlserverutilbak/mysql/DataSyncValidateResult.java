package com.example.mysqlserverutilbak.mysql;

import com.google.common.collect.Maps;
import lombok.Data;

import java.util.Map;

import static com.example.mysqlserverutilbak.mysql.DifferenceInfo.*;

@Data
public class DataSyncValidateResult {
    /**
     * CheckResult的作用：
     * 1. According to dbName + table, get diffInfo    // 用于关注core表，更加直观
     * 2. get diffInfos in DiffType    // 用于展示差异项，更加清晰。
     */

    // key = DiffType, value = <DiffKey, differenceInfo>
    private Map<DiffType, Map<String, DifferenceInfo>> differences = Maps.newHashMap();

    public void registerDiffInfo(DifferenceInfo differenceInfo) {
        Map<String, DifferenceInfo> diffMap = differences.get(differenceInfo.getDiffType());
        if (diffMap == null || diffMap.isEmpty()) {
            diffMap = Maps.newHashMap();
            differences.put(differenceInfo.getDiffType(), diffMap);
        }

        diffMap.put(differenceInfo.getKey(), differenceInfo);
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

    public Map<DiffType, Map<String, DifferenceInfo>> getDifferences() {
        return differences;
    }
}
