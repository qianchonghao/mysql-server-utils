package com.example.mysqlserverutilbak.mysql;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets.SetView;

import static com.example.mysqlserverutilbak.mysql.util.SqlUtils.*;
import static com.example.mysqlserverutilbak.mysql.DifferenceInfo.*;

/**
 * @Author qch
 * @Date 2022/8/12 11:58 上午
 */
@Configuration
// @Leimo todo: logback-spring.xml需要修改存储log文件的路径。
@Slf4j
public class DataSyncValidator implements InitializingBean {
    private static final Set EXCLUSIVE_DATABASES = ImmutableSet.of("mysql", "performance_schema", "sys", "information_schema");

    private static final String SHOW_DB = "show databases;";

    private static final String SHOW_TABLES = "SHOW TABLES FROM ";

    private static final String SHOW_TABLE_STRUCTURES = "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = ? AND TABLE_NAME in ";

    private static final String SHOW_TABLE_STRUCTURES_IN_DB = "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = ? limit ?,?";

    private static final int LIMIT = 500;
    @Autowired
    private DataSource sourceDataSource;

    @Autowired
    private DataSource targetDataSource;


    private Set<String> sourceDBNames = Sets.newHashSet();
    private Set<String> targetDBNames = Sets.newHashSet();

    @Override
    public void afterPropertiesSet() {
        DataSyncValidateResult res = new DataSyncValidateResult();

        // 1. [MISS_DATABASE]： 构建差集，关注 source具备，但是target不具备的 datasource。
        targetDBNames = getAllDBInDataSource(targetDataSource);
        sourceDBNames = getAllDBInDataSource(sourceDataSource);
        // rawSource 标记未被处理的 db.table
        Map<String, Set<String>> rawSource = sourceDBNames.stream().collect(Collectors.toMap(
                dbName -> dbName,
                dbName -> Sets.newHashSet()
        ));

        // 若存在 target具备，但是 source不具备的db。不需要关心，本质是保证 source数据全部同步到target
        SetView<String> difference = Sets.difference(sourceDBNames, targetDBNames);
        if (!difference.isEmpty()) {
            log.info("[DBCheck result]: target db differ from source, difference = {}", difference);
            difference.stream().forEach(dbName -> {
                rawSource.remove(dbName);
                res.registerDiffInfo(new DBMissInfo(dbName));
            });
        }

        // 2. [MISS_TABLE]: 同db下，source存在，但target不具备的table
        Map<String, Set<String>> tempRawSource = Maps.newHashMap();

        for (String dbName : rawSource.keySet()) {
            Set<String> sourceTables = getTables(sourceDataSource, dbName);
            Set<String> targetTables = getTables(targetDataSource, dbName);
            SetView<String> tableDifference = Sets.difference(sourceTables, targetTables);

            //rawTables 标记 dbNames下未处理的table
            Set<String> rawTables = Sets.newHashSet();
            rawTables.addAll(sourceTables);

            if (!tableDifference.isEmpty()) {
                rawTables.removeAll(tableDifference);
                for (String tableName : tableDifference) {
                    res.registerDiffInfo(new TableMissInfo(dbName, tableName));
                }
            }
            tempRawSource.put(dbName, rawTables);
        }

        mergeTemp2RawSource(rawSource, tempRawSource);

        // 3. [DIFF_FROM_TABLE_STRUCTURE]: db.table为key，对比source, target的表结构
        //  (1) db数量控制在54，执行54次循环。(2) 单个db内平均50张table。(3) table 总量在千级,约2500 (4) 每张table 约10个column，单db下 500+column。
        // batch query column from information_schema,     single column query会给予数据库过大压力。
        // SQL 语句中 in参数的限制：     https://blog.csdn.net/a772304419/article/details/103838176#:~:text=Oracle%E4%B8%AD%20%EF%BC%8Cin%E8%AF%AD%E5%8F%A5%E4%B8%AD%E5%8F%AF%E6%94%BE%E7%9A%84%E6%9C%80%E5%A4%A7%E5%8F%82%E6%95%B0%E4%B8%AA%E6%95%B0%E6%98%AF%201000%E4%B8%AA%20%E3%80%82%20%E4%B9%8B%E5%89%8D%E9%81%87%E5%88%B0%E8%B6%85%E8%BF%871000%E7%9A%84%E6%83%85%E5%86%B5%EF%BC%8C%E5%8F%AF%E7%94%A8%E5%A6%82%E4%B8%8B%E8%AF%AD%E5%8F%A5%EF%BC%8C%E4%BD%86%E5%A6%82%E6%AD%A4%E5%A4%9A%E5%8F%82%E6%95%B0%E9%A1%B9%E7%9B%AE%E4%BC%9A%E4%BD%8E%EF%BC%8C%E5%8F%AF%E8%80%83%E8%99%91%E7%94%A8%E5%88%AB%E7%9A%84%E6%96%B9%E5%BC%8F%E4%BC%98%E5%8C%96%E3%80%82%20select%20%2A%20where,id%20%28xxx%2Cxxx...%29%20id%20%28yyy%2Cyyy%2C...%29%20mysql%E4%B8%AD%20%EF%BC%8Cin%E8%AF%AD%E5%8F%A5%E4%B8%AD%E5%8F%82%E6%95%B0%E4%B8%AA%E6%95%B0%E6%98%AF%20%E4%B8%8D%E9%99%90%E5%88%B6%20%E7%9A%84%E3%80%82

        Map<String, Map<String, Set<ColumnStructure>>> sourceColumnMap = getColumnsInfoFromDataSource(rawSource, sourceDataSource);
        Map<String, Map<String, Set<ColumnStructure>>> targetColumnMap = getColumnsInfoFromDataSource(rawSource, targetDataSource);
        tempRawSource = Maps.newHashMap();
        for (Entry<String, Map<String, Set<ColumnStructure>>> dbEntry : sourceColumnMap.entrySet()) {
            String dbName = dbEntry.getKey();
            Map<String, Set<ColumnStructure>> tableColumns = dbEntry.getValue();
            Set<String> rawTables = Sets.newHashSet(tableColumns.keySet());

            for (Entry<String, Set<ColumnStructure>> tableEntry : tableColumns.entrySet()) {
                String tableName = tableEntry.getKey();
                Set<ColumnStructure> sourceColumns = tableEntry.getValue();
                Set<ColumnStructure> targetColumns = targetColumnMap.computeIfAbsent(dbName, key -> Maps.newHashMap()).get(tableName);
                // @leimo todo: source和target存在 db.table.columnName一致，但structure整体存在差异的columns。note: Pair 形式存储。
                Sets.SetView<ColumnStructure> columnsOnlyInSource = Sets.difference(sourceColumns, targetColumns);
                Sets.SetView<ColumnStructure> columnsOnlyInTarget = Sets.difference(targetColumns, sourceColumns);
                if (!columnsOnlyInSource.isEmpty() || !columnsOnlyInTarget.isEmpty()) {
                    TableStructureDiffInfo diffInfo = new TableStructureDiffInfo(dbName, tableName);
                    diffInfo.setColumnsOnlyInSource(Lists.newArrayList(columnsOnlyInSource));
                    diffInfo.setColumnsOnlyInTarget(Lists.newArrayList(columnsOnlyInTarget));
                    res.registerDiffInfo(diffInfo);

                    rawTables.remove(tableName);
                }
            }
            tempRawSource.put(dbName, rawTables);
        }
        mergeTemp2RawSource(rawSource, tempRawSource);

        // 4. [DIFF_FROM_RECORD_NUM]
        // UNION ALL拼接多table count， LIMIT = 500 统计record count
//        SELECT count(*) total_count, 'columns' table_name FROM information_schema.columns
//        UNION ALL
//        SELECT count(*) total_count, '_test_leimo_user' table_name FROM xspace_account._test_leimo_user
//        @leimo note: 字符串的最大长度：https://www.cnblogs.com/54chensongxia/p/13640352.html#:~:text=String%20%E7%9A%84%E9%95%BF%E5%BA%A6%E6%98%AF%E6%9C%89%E9%99%90%E5%88%B6%E7%9A%84%E3%80%82,%E7%BC%96%E8%AF%91%E6%9C%9F%E7%9A%84%E9%99%90%E5%88%B6%EF%BC%9A%E5%AD%97%E7%AC%A6%E4%B8%B2%E7%9A%84UTF8%E7%BC%96%E7%A0%81%E5%80%BC%E7%9A%84%E5%AD%97%E8%8A%82%E6%95%B0%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%8765535%EF%BC%8C%E5%AD%97%E7%AC%A6%E4%B8%B2%E7%9A%84%E9%95%BF%E5%BA%A6%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%8765534%EF%BC%9B%20%E8%BF%90%E8%A1%8C%E6%97%B6%E9%99%90%E5%88%B6%EF%BC%9A%E5%AD%97%E7%AC%A6%E4%B8%B2%E7%9A%84%E9%95%BF%E5%BA%A6%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%872%5E31-1%EF%BC%8C%E5%8D%A0%E7%94%A8%E7%9A%84%E5%86%85%E5%AD%98%E6%95%B0%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%87%E8%99%9A%E6%8B%9F%E6%9C%BA%E8%83%BD%E5%A4%9F%E6%8F%90%E4%BE%9B%E7%9A%84%E6%9C%80%E5%A4%A7%E5%80%BC%E3%80%82

        Map<String, Map<String, Long>> sourceRecordCount = getTableCountMap(rawSource, sourceDataSource);
        Map<String, Map<String, Long>> targetRecordCount = getTableCountMap(rawSource, targetDataSource);
        tempRawSource = Maps.newHashMap();

        for (Entry<String, Map<String, Long>> entry : sourceRecordCount.entrySet()) {
            String dbName = entry.getKey();
            Map<String, Long> tableCountMap = entry.getValue();
            Set<String> rawTables = Sets.newHashSet(tableCountMap.keySet());

            for (Entry<String, Long> tableCountEntry : tableCountMap.entrySet()) {
                String tableName = tableCountEntry.getKey();
                Long sourceTotalCount = tableCountEntry.getValue();
                Long targetTotalCount = targetRecordCount.computeIfAbsent(dbName, key -> Maps.newHashMap()).get(tableName);
                if (sourceTotalCount == null || !sourceTotalCount.equals(targetTotalCount)) {
                    RecordDiffInfo diffInfo = new RecordDiffInfo(dbName, tableName);
                    diffInfo.setSourceTotalCount(sourceTotalCount);
                    diffInfo.setTargetTotalCount(targetTotalCount);
                    res.registerDiffInfo(diffInfo);

                    rawTables.remove(tableName);
                }
            }
            tempRawSource.put(dbName, rawTables);
        }
        mergeTemp2RawSource(rawSource, tempRawSource);


        // 5. [DIFF_FROM_RECORD_CONTENT]
        // @leimo todo: 如何判断record不等价？ 是否ReturnSet能否转换成 标准Record
        // rs.getType 返回 数据表类型code，（1）根据type_code执行反序列化。 （2）拼接 record 对象 （3）执行equals方法
        log.info(">>>>>>>>>>> [DataSyncValidator] result is {}", res);
    }

    private Map<String, Map<String, Long>> getTableCountMap(Map<String, Set<String>> rawSource, DataSource dataSource) {
        Map<String, Map<String, Long>> recordCount = Maps.newHashMap();
        for (Entry<String, Set<String>> entry : rawSource.entrySet()) {
            String dbName = entry.getKey();
            Set<String> tables = entry.getValue();
            executeQuery(dataSource, getDBCountSQL(dbName, tables), (rs) -> {
                try {
                    while (rs.next()) {

                        String tableName = rs.getString("table_name");
                        Long totalCount = rs.getLong("total_count");
                        Map<String, Long> tableRecordCount = recordCount.computeIfAbsent(dbName, (key) -> Maps.newHashMap());
                        tableRecordCount.put(tableName, totalCount);
                    }
                } catch (SQLException e) {
                    log.error("[DIFF_FROM_RECORD_NUM] fail", e);
                }
                return null;
            });
        }
        return recordCount;
    }

    private String getDBCountSQL(String dbName, Set<String> tables) {
        StringBuilder sb = new StringBuilder();
        if (StringUtils.isEmpty(dbName) || tables == null || tables.isEmpty()) {
            return null;
        }
        tables.stream().forEach((tableName) -> {
            if (!sb.toString().isEmpty()) {
                sb.append("UNION ALL \n");
            }
            sb.append("SELECT ").
                    append("'").append(dbName).append("' ").append("db_name, ").
                    append("'").append(tableName).append("' ").append("table_name, ").
                    append("count(*) total_count FROM ").append(linkDBAndTable(dbName, tableName)).append(" \n");
        });
        return sb.toString();
    }

    public static String linkDBAndTable(String dbName, String tableName) {
        String res = null;

        if (dbName.contains("-")) {
            res = "`" + dbName + "`" + "." + tableName;
        } else {
            res = dbName + "." + tableName;
        }
        return res;
    }

    private void mergeTemp2RawSource(Map<String, Set<String>> rawSource, Map<String, Set<String>> tempRawSource) {
        for (Entry<String, Set<String>> entry : tempRawSource.entrySet()) {
            String dbName = entry.getKey();
            Set<String> tables = entry.getValue();
            if (tables.isEmpty()) {
                rawSource.remove(dbName);
            } else {
                rawSource.put(dbName, tables);
            }
        }
    }

    /**
     * 查询表结构的条件 ： TABLE_SCHEMA, TABLE_NAME
     * columns 表内，必须一致的columns结构。
     * 1. COLUMN_NAME                  列名
     * 2. COLUMN_DEFAULT               默认值
     * 3. IS_NULLABLE                  是否可为null
     * 4. DATA_TYPE                    mysql 数据类型
     * 5. CHARACTER_MAXIMUM_LENGTH     type=varchar, 最大字段长度。记录字符最大长度
     * 6. CHARACTER_OCTET_LENGTH       length in bytes，记录字节最大长度
     * 7. NUMERIC_PRECISION            数据精度
     * 8. NUMERIC_SCALE                数据范围
     * 9. DATETIME_PRECISION           日期精度
     * 10. CHARACTER_SET_NAME          字符编码的字符集
     * 11. COLLATION_NAME              物理存储的排序规则
     * collation_name实例:  utf8_general_ci（utf8指排序规则支持的字符集，general_ci指不区分大小写）
     * https://blog.csdn.net/wei198621/article/details/1880814
     * 12. COLUMN_TYPE                 列数据类型
     * 13. COLUMN_KEY                  PRI(主键的组成部分), UNI（唯一索引的组成部分，not null）, MUL（非唯一索引组成部分 or 唯一索引可为null部分）,
     * https://blog.csdn.net/Seraph_fd/article/details/48750335
     * 14. PRIVILEGES                  tables or columns的特权
     * https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_references
     * 15. ORDINAL_POSITION            columns的位置
     */
    private Map<String, Map<String, Set<ColumnStructure>>> getColumnsInfoFromDataSource(Map<String, Set<String>> rawSource, DataSource dataSource) {
        Map<String, Map<String, Set<ColumnStructure>>> tableStructure = Maps.newHashMap();
        for (Entry<String, Set<String>> entry : rawSource.entrySet()) {
            String dbName = entry.getKey();
            Set<String> tables = entry.getValue();
            Integer start = 0;

            for (int i = 0; start == i * LIMIT; i++) {
                Integer columnTotal = executeQuery(dataSource, SHOW_TABLE_STRUCTURES_IN_DB, (rs) -> {
                    int columnTotal0 = 0;
                    try {
                        while (rs.next()) {// 存储单个column的结构
                            columnTotal0++;
                            String tableName = rs.getString("TABLE_NAME");
                            ColumnStructure columnStructure = new ColumnStructure();
                            columnStructure.setTableSchema(dbName);
                            columnStructure.setTableName(tableName);
                            columnStructure.setColumnName(rs.getString("COLUMN_NAME"));
                            columnStructure.setColumnDefault(rs.getString("COLUMN_DEFAULT"));
                            columnStructure.setIsNullable(rs.getString("IS_NULLABLE"));
                            columnStructure.setDataType(rs.getString("DATA_TYPE"));
                            columnStructure.setCharacterMaximumLength(rs.getLong("CHARACTER_MAXIMUM_LENGTH"));
                            columnStructure.setCharacterOctetLength(rs.getLong("CHARACTER_OCTET_LENGTH"));
                            columnStructure.setNumericPrecision(rs.getLong("NUMERIC_PRECISION"));
                            columnStructure.setNumericScale(rs.getLong("NUMERIC_SCALE"));
                            columnStructure.setDataTimePrecision(rs.getString("DATETIME_PRECISION"));
                            columnStructure.setCharacterSetName(rs.getString("CHARACTER_SET_NAME"));
                            columnStructure.setCollationName(rs.getString("COLLATION_NAME"));
                            columnStructure.setColumnType(rs.getString("COLUMN_TYPE"));
                            columnStructure.setColumnKey(rs.getString("COLUMN_KEY"));
                            columnStructure.setPrivileges(rs.getString("PRIVILEGES"));
                            columnStructure.setOrdinalPosition(rs.getLong("ORDINAL_POSITION"));

                            Set<ColumnStructure> columnStructureSet = tableStructure.
                                    computeIfAbsent(dbName, dbName0 -> Maps.newHashMap()).
                                    computeIfAbsent(tableName, tableName0 -> Sets.newHashSet());
                            columnStructureSet.add(columnStructure);
                        }
                    } catch (SQLException e) {
                        log.error("[ShowTableStructure] SQL execute fail", e);
                    }
                    return columnTotal0;
                }, dbName, start, LIMIT);
                start += columnTotal;
            }
        }
        return tableStructure;
    }

    private String getShowTableStructureSQL(Set<String> tables) {
        StringBuilder sb = new StringBuilder();

        tables.stream().forEach((tableName) -> {
            if (sb.toString().isEmpty()) {
                sb.append(SHOW_TABLE_STRUCTURES).append("(");
            } else {
                sb.append(",");
            }
            sb.append("\'").append(tableName).append("\'");
        });

        sb.append(")").append(" limit ?,").append(LIMIT);

        return sb.toString();
    }

    private Set<String> getTables(DataSource dataSource, String dbName) {
        Set<String> tableNames = executeQuery(dataSource, buildShowTablesSQL(dbName), (rs) -> {
            Set<String> tableNames0 = Sets.newHashSet();
            try {
                while (rs.next()) {
                    String tableName = rs.getString(1);
                    tableNames0.add(tableName);
                }
            } catch (SQLException e) {
                log.error("[DBCheck]: query tables fail", e);
            }
            return tableNames0;
        });

        return tableNames;
    }

    private String buildShowTablesSQL(String dbName) {
        if (!dbName.contains("-")) {
            return SHOW_TABLES + dbName;
        } else {
            return SHOW_TABLES + "`" + dbName + "`";
        }

    }

    private Set<String> getAllDBInDataSource(DataSource dataSource) {
        Set<String> dbNames = executeQuery(dataSource, SHOW_DB, (rs) -> {
            Set<String> dbNames0 = Sets.newHashSet();
            try {
                while (rs.next()) {
                    String dbName = rs.getString(1);
                    if (!EXCLUSIVE_DATABASES.contains(dbName)) {
                        dbNames0.add(dbName);
                    }
                }
            } catch (SQLException e) {
                log.error("[DataSyncValidator]: getAllDBInDataSource fail", e);
            }
            return dbNames0;
        });
        return dbNames;
    }
}
