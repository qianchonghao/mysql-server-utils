package com.example.mysqlserverutilbak.mysql;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.example.mysqlserverutilbak.mysql.util.SqlUtils.executeQuery;

/**
 * @Author qch
 * @Date 2022/8/16 3:12 下午
 */
@Component
@Slf4j
public class DBQueryService {
    private static final Set EXCLUSIVE_DATABASES = ImmutableSet.of("mysql", "performance_schema", "sys", "information_schema");

    private static final Set STRING_TYPE = ImmutableSet.of("varchar", "longtext", "tinytext", "mediumtext", "text");

    private static final String SHOW_DB = "show databases;";

    private static final String SHOW_TABLES = "SHOW TABLES FROM ";

    private static final String SHOW_TABLE_STRUCTURES_IN_DB = "SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA = ? limit ?,?";

    public static final int LIMIT = 500;

    // 1. show database
    public Set<String> getAllDBInDataSource(DataSource dataSource) {
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
                log.error("[SqlBuilder]: getAllDBInDataSource fail", e);
            }
            return dbNames0;
        });
        return dbNames;
    }

    // 2. show tables from ${db_name}
    public Set<String> getTables(DataSource dataSource, String dbName) {
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
        return SHOW_TABLES + enhanceWithBackQuote(dbName);
    }

    private String enhanceWithBackQuote(String str) {
        return "`" + str + "`";
    }

    private String enhanceWithQuote(String str) {
        return "'" + str + "'";
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
    // 3. 查询db下所有table的表结构  SELECT * FROM information_schema where table_schema = ${db_name}
    public Map<String, Map<String, Set<ColumnStructure>>> getColumnsInfoFromDataSource(Map<String, Set<String>> rawSource, DataSource dataSource) {
        Map<String, Map<String, Set<ColumnStructure>>> tableStructure = Maps.newHashMap();
        for (Map.Entry<String, Set<String>> entry : rawSource.entrySet()) {
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

    // 4. 统计db下所有table的 record count
    public Map<String, Map<String, Long>> getTableCountMap(Map<String, Set<String>> rawSource, DataSource dataSource) {
        Map<String, Map<String, Long>> recordCount = Maps.newHashMap();
        for (Map.Entry<String, Set<String>> entry : rawSource.entrySet()) {
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
                    append(enhanceWithBackQuote(dbName)).append("db_name, ").
                    append("'").append(tableName).append("' ").append("table_name, ").
                    append("count(*) total_count FROM ").append(getDBTableKey(dbName, tableName)).append(" \n");
        });
        return sb.toString();
    }

    private String getDBTableKey(String dbName, String tableName) {
        return enhanceWithBackQuote(dbName) + "." + enhanceWithBackQuote(tableName);
    }

    // 5. 批量顺序查询 source records， 每批次500条
    public List<Record> querySourceRecords(DataSource sourceDataSource, List<ColumnStructure> primaryColStructures, int start) {
        Pair<String, String> db2Table = getDBInfoInStructure(primaryColStructures);
        String dbName = db2Table.getLeft();
        String tableName = db2Table.getRight();

        return executeQuery(sourceDataSource,
                getQueryRecordsInOrderSQL(dbName, tableName),
                rs -> getRecordsFromResultSet(primaryColStructures, rs), start, LIMIT);
    }

    private Pair<String, String> getDBInfoInStructure(List<ColumnStructure> colStructures) {
        ColumnStructure columnStructure = colStructures.stream().findFirst().orElse(null);
        if (columnStructure == null) {
            log.error("getDBTableNameInStructure fail");
        }

        String dbName = columnStructure.getTableSchema();
        String tableName = columnStructure.getTableName();
        return Pair.of(dbName, tableName);
    }

    private String getQueryRecordsInOrderSQL(String dbName, String tableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM ").append(getDBTableKey(dbName, tableName)).append(" LIMIT ?,?");
        return sb.toString();
    }

    private List<Record> getRecordsFromResultSet(List<ColumnStructure> primaryColStructures, ResultSet rs) {
        List<Record> records = Lists.newArrayList();
        try {
            while (rs.next()) {
                Record record = new Record();
                List<Object> columnValues = Lists.newArrayList();
                List<Pair<Object, ColumnStructure>> primaryColValues = Lists.newArrayList();
                Map<String, ColumnStructure> primaryKeyMap = primaryColStructures.stream().collect(
                        Collectors.toMap(columnStructure -> columnStructure.getColumnName(), columnStructure -> columnStructure));


                ResultSetMetaData metaData = rs.getMetaData();
                for (int index = 1; index <= metaData.getColumnCount(); index++) {
                    Object columnValue = rs.getObject(index);
                    columnValues.add(columnValue);
                    String columnName = metaData.getColumnLabel(index);
                    if (primaryKeyMap.keySet().contains(columnName)) {
                        Pair<Object, ColumnStructure> value2Structure = Pair.of(columnValue, primaryKeyMap.get(columnName));
                        primaryColValues.add(value2Structure);
                    }
                }

                record.setColumnValues(columnValues);
                record.setPrimaryColValues(primaryColValues);
                records.add(record);
            }

        } catch (SQLException e) {
            log.error("[query records fail]", e);
        }
        return records;
    }

    // 6. 根据source records 提供的 primaryKeys，查询target
    public List<Record> queryTargetRecords(DataSource targetDataSource, List<ColumnStructure> primaryColStructures, int start, List<Record> sourceRecords) {
        Pair<String, String> db2Table = getDBInfoInStructure(primaryColStructures);
        String dbName = db2Table.getLeft();
        String tableName = db2Table.getRight();

        return executeQuery(targetDataSource,
                getQueryRecordsInPKSQL(dbName, tableName, primaryColStructures, sourceRecords),
                rs -> getRecordsFromResultSet(primaryColStructures, rs), start, LIMIT);
    }

    private String getQueryRecordsInPKSQL(String dbName, String tableName, List<ColumnStructure> primaryColStructures, List<Record> sourceRecords) {
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT * FROM ").
                append(getDBTableKey(dbName, tableName)).
                append(" WHERE ").
                append(defaultBuildInCondition(primaryColStructures, columnStructure -> enhanceWithBackQuote(columnStructure.getColumnName()))).
                append(" in ").
                append(defaultBuildInCondition(sourceRecords, record ->
                        defaultBuildInCondition(record.getPrimaryColValues(), (pkPair) -> {
                            // @leimo todo : pk的type支持 varchar，bigInt等, Map<type, @sqlStringBuilder@>
                            Object value = pkPair.getLeft();
                            ColumnStructure columnStructure = pkPair.getRight();
                            String res = value.toString();
                            if (STRING_TYPE.contains(columnStructure.getColumnName())) {
                                res = enhanceWithQuote(value.toString());
                            }
                            return res;
                        }))).
                append(" LIMIT ?,?");
        return sb.toString();
    }


    public abstract class MysqlTypeHandler {
        abstract String getType();
    }

    public class DefaultHandler extends MysqlTypeHandler {

        @Override
        String getType() {
            return null;
        }
    }

    private <T> String defaultBuildInCondition(Collection<T> collection, Function<T, String> partStrProvider) {
        return buildInCondition(collection, partStrProvider, "(", ",", ")");
    }

    private <T> String buildInCondition(Collection<T> collection, Function<T, String> partStrProvider, String begin, String separator, String end) {
        if (collection == null || collection.isEmpty()) {
            return "(NULL)";
        }
        Iterator<T> it = collection.iterator();

        StringBuilder res = new StringBuilder();
        res.append(begin);

        while (it.hasNext()) {
            T ele = it.next();
            String content = partStrProvider.apply(ele);
            res.append(content);
            if (it.hasNext()) {
                res.append(separator);
            }
        }

        res.append(end);
        return res.toString();
    }
}
