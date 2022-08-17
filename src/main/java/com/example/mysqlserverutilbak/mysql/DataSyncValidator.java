package com.example.mysqlserverutilbak.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import com.google.common.collect.Sets.SetView;
import org.springframework.util.StopWatch;

import static com.example.mysqlserverutilbak.mysql.DifferenceInfo.*;
import static com.example.mysqlserverutilbak.mysql.ColumnStructure.*;
import static com.example.mysqlserverutilbak.mysql.DBQueryService.*;
import static com.example.mysqlserverutilbak.mysql.Record.*;

/**
 * @Author qch
 * @Date 2022/8/12 11:58 上午
 */
@Configuration
// @Leimo todo: logback-spring.xml需要修改存储log文件的路径。
@Slf4j
public class DataSyncValidator implements InitializingBean {

    private static int RECORD_STORAGE_LIMIT = 20;

    @Autowired
    private ValidateConfig config;

    @Autowired
    private DataSource sourceDataSource;

    @Autowired
    private DataSource targetDataSource;

    @Autowired
    private DiffInfoRegistry registry;

    @Autowired
    DBQueryService DBQueryService;

    private ThreadPoolExecutor executor;

    @Override
    public void afterPropertiesSet() {
        StopWatch stopWatch = new StopWatch();

        stopWatch.start();
        // 1. [MISS_DATABASE]： 构建差集，关注 source具备，但是target不具备的 datasource。
        Set<String> targetDBNames = DBQueryService.getAllDBInDataSource(targetDataSource);
        Set<String> sourceDBNames = DBQueryService.getAllDBInDataSource(sourceDataSource);
        stopWatch.stop();
        log.info("[MISS_DATABASE-0]: the consuming is {} ms", stopWatch.getLastTaskTimeMillis());
        stopWatch.start();
        // rawSource 标记未被处理的 db.table
        Map<String, Set<String>> rawSource = sourceDBNames.stream().collect(Collectors.toConcurrentMap(
                dbName -> dbName,
                dbName -> Sets.newHashSet()
        ));

        // 若存在 target具备，但是 source不具备的db。不需要关心，本质是保证 source数据全部同步到target
        SetView<String> difference = Sets.difference(sourceDBNames, targetDBNames);
        if (!difference.isEmpty()) {
            log.info("[DBCheck result]: target db differ from source, difference = {}", difference);
            difference.stream().forEach(dbName -> {
                rawSource.remove(dbName);
                registry.registerDiffInfo(new DBMissInfo(dbName));
            });
        }
        stopWatch.stop();
        log.info("[MISS_DATABASE-1]: the consuming is {} ms", stopWatch.getLastTaskTimeMillis());

        stopWatch.start();
        // 2. [MISS_TABLE]: 同db下，source存在，但target不具备的table
        Map<String, Set<String>> tempRawSource = Maps.newHashMap();

        for (String dbName : rawSource.keySet()) {
            Set<String> sourceTables = DBQueryService.getTables(sourceDataSource, dbName);
            Set<String> targetTables = DBQueryService.getTables(targetDataSource, dbName);
            SetView<String> tableDifference = Sets.difference(sourceTables, targetTables);

            //rawTables 标记 dbNames下未处理的table
            Set<String> rawTables = Sets.newHashSet();
            rawTables.addAll(sourceTables);

            if (!tableDifference.isEmpty()) {
                rawTables.removeAll(tableDifference);
                for (String tableName : tableDifference) {
                    registry.registerDiffInfo(new TableMissInfo(dbName, tableName));
                }
            }
            tempRawSource.put(dbName, rawTables);
        }

        mergeTemp2RawSource(rawSource, tempRawSource);
        stopWatch.stop();
        log.info("[MISS_TABLE]: the consuming is {} ms", stopWatch.getLastTaskTimeMillis());

        // 3. [DIFF_FROM_TABLE_STRUCTURE]: db.table为key，对比source, target的表结构
        //  (1) db数量控制在54，执行54次循环。(2) 单个db内平均50张table。(3) table 总量在千级,约2500 (4) 每张table 约10个column，单db下 500+column。
        // batch query column from information_schema,     single column query会给予数据库过大压力。
        // SQL 语句中 in参数的限制：     https://blog.csdn.net/a772304419/article/details/103838176#:~:text=Oracle%E4%B8%AD%20%EF%BC%8Cin%E8%AF%AD%E5%8F%A5%E4%B8%AD%E5%8F%AF%E6%94%BE%E7%9A%84%E6%9C%80%E5%A4%A7%E5%8F%82%E6%95%B0%E4%B8%AA%E6%95%B0%E6%98%AF%201000%E4%B8%AA%20%E3%80%82%20%E4%B9%8B%E5%89%8D%E9%81%87%E5%88%B0%E8%B6%85%E8%BF%871000%E7%9A%84%E6%83%85%E5%86%B5%EF%BC%8C%E5%8F%AF%E7%94%A8%E5%A6%82%E4%B8%8B%E8%AF%AD%E5%8F%A5%EF%BC%8C%E4%BD%86%E5%A6%82%E6%AD%A4%E5%A4%9A%E5%8F%82%E6%95%B0%E9%A1%B9%E7%9B%AE%E4%BC%9A%E4%BD%8E%EF%BC%8C%E5%8F%AF%E8%80%83%E8%99%91%E7%94%A8%E5%88%AB%E7%9A%84%E6%96%B9%E5%BC%8F%E4%BC%98%E5%8C%96%E3%80%82%20select%20%2A%20where,id%20%28xxx%2Cxxx...%29%20id%20%28yyy%2Cyyy%2C...%29%20mysql%E4%B8%AD%20%EF%BC%8Cin%E8%AF%AD%E5%8F%A5%E4%B8%AD%E5%8F%82%E6%95%B0%E4%B8%AA%E6%95%B0%E6%98%AF%20%E4%B8%8D%E9%99%90%E5%88%B6%20%E7%9A%84%E3%80%82
        stopWatch.start();
        Map<String, Map<String, Set<ColumnStructure>>> sourceColumnMap = DBQueryService.getColumnsInfoFromDataSource(rawSource, sourceDataSource);
        Map<String, Map<String, Set<ColumnStructure>>> targetColumnMap = DBQueryService.getColumnsInfoFromDataSource(rawSource, targetDataSource);
        tempRawSource = Maps.newHashMap();
        for (Entry<String, Map<String, Set<ColumnStructure>>> dbEntry : sourceColumnMap.entrySet()) {
            String dbName = dbEntry.getKey();
            Map<String, Set<ColumnStructure>> tableColumns = dbEntry.getValue();
            Set<String> rawTables = Sets.newHashSet(tableColumns.keySet());

            for (Entry<String, Set<ColumnStructure>> tableEntry : tableColumns.entrySet()) {
                String tableName = tableEntry.getKey();
                Set<ColumnStructure> sourceColumns = tableEntry.getValue();
                Set<ColumnStructure> targetColumns = targetColumnMap.computeIfAbsent(dbName, key -> Maps.newHashMap()).get(tableName);
                Sets.SetView<ColumnStructure> columnsOnlyInSource = Sets.difference(sourceColumns, targetColumns);
                Sets.SetView<ColumnStructure> columnsOnlyInTarget = Sets.difference(targetColumns, sourceColumns);
                if (!columnsOnlyInSource.isEmpty() || !columnsOnlyInTarget.isEmpty()) {
                    TableStructureDiffInfo diffInfo = new TableStructureDiffInfo(dbName, tableName);
                    diffInfo.setColumnsOnlyInSource(Lists.newArrayList(columnsOnlyInSource));
                    diffInfo.setColumnsOnlyInTarget(Lists.newArrayList(columnsOnlyInTarget));
                    registry.registerDiffInfo(diffInfo);

                    rawTables.remove(tableName);
                }
            }
            tempRawSource.put(dbName, rawTables);
        }
        mergeTemp2RawSource(rawSource, tempRawSource);
        stopWatch.stop();
        log.info("[DIFF_FROM_TABLE_STRUCTURE]: the consuming is {} ms", stopWatch.getLastTaskTimeMillis());
        // 4. [DIFF_FROM_RECORD_NUM]
        // UNION ALL拼接多table count， LIMIT = 500 统计record count
//        SELECT count(*) total_count, 'columns' table_name FROM information_schema.columns
//        UNION ALL
//        SELECT count(*) total_count, '_test_leimo_user' table_name FROM xspace_account._test_leimo_user
//        @leimo note: 字符串的最大长度：https://www.cnblogs.com/54chensongxia/p/13640352.html#:~:text=String%20%E7%9A%84%E9%95%BF%E5%BA%A6%E6%98%AF%E6%9C%89%E9%99%90%E5%88%B6%E7%9A%84%E3%80%82,%E7%BC%96%E8%AF%91%E6%9C%9F%E7%9A%84%E9%99%90%E5%88%B6%EF%BC%9A%E5%AD%97%E7%AC%A6%E4%B8%B2%E7%9A%84UTF8%E7%BC%96%E7%A0%81%E5%80%BC%E7%9A%84%E5%AD%97%E8%8A%82%E6%95%B0%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%8765535%EF%BC%8C%E5%AD%97%E7%AC%A6%E4%B8%B2%E7%9A%84%E9%95%BF%E5%BA%A6%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%8765534%EF%BC%9B%20%E8%BF%90%E8%A1%8C%E6%97%B6%E9%99%90%E5%88%B6%EF%BC%9A%E5%AD%97%E7%AC%A6%E4%B8%B2%E7%9A%84%E9%95%BF%E5%BA%A6%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%872%5E31-1%EF%BC%8C%E5%8D%A0%E7%94%A8%E7%9A%84%E5%86%85%E5%AD%98%E6%95%B0%E4%B8%8D%E8%83%BD%E8%B6%85%E8%BF%87%E8%99%9A%E6%8B%9F%E6%9C%BA%E8%83%BD%E5%A4%9F%E6%8F%90%E4%BE%9B%E7%9A%84%E6%9C%80%E5%A4%A7%E5%80%BC%E3%80%82

        stopWatch.start();
        Map<String, Map<String, Long>> sourceRecordCount = DBQueryService.getTableCountMap(rawSource, sourceDataSource);
        Map<String, Map<String, Long>> targetRecordCount = DBQueryService.getTableCountMap(rawSource, targetDataSource);
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
                    RecordCountDiffInfo diffInfo = new RecordCountDiffInfo(dbName, tableName);
                    diffInfo.setSourceTotalCount(sourceTotalCount);
                    diffInfo.setTargetTotalCount(targetTotalCount);
                    registry.registerDiffInfo(diffInfo);

                    rawTables.remove(tableName);
                }
            }
            tempRawSource.put(dbName, rawTables);
        }
        mergeTemp2RawSource(rawSource, tempRawSource);
        stopWatch.stop();
        log.info("[DIFF_FROM_RECORD_NUM]: the consuming is {} ms", stopWatch.getLastTaskTimeMillis());

        // 5. [DIFF_FROM_RECORD_CONTENT]
        // rs.getType 返回 数据表类型code，（1）根据type_code执行反序列化。 （2）拼接 record 对象 （3）执行equals方法

        // 目标-1：查询source & target 数据源内的 db.table的数据
        // 1. query source records limit start,start+500  500条做比较，避免内存存储的数据过多。 @leimo question: 内存能否容纳大量数据？
        // 2. 遍历source， 构造唯一索引的keySet
        // 3. query target records in keySet (数据库查询)

        // 目标-2：比较 source & target的 records
        // 1. target_count == 500;  唯一索引查询出500条数据
        // 2. 根据 sourceColumns.dataType 实现反序列化，构造 java record对象

        // 目标-3： 以db.table为基本单位，分配线程执行records compare. countDownLatch实现主线程阻塞等待多线程并发compare records
        stopWatch.start();
        int rawTableNum = rawSource.values().stream().map(set -> set.size()).reduce((total0, ele) -> total0 + ele).orElse(0);
        log.info("[DIFF_FROM_RECORD_CONTENT] raw source table count = {}", rawTableNum);
        CountDownLatch latch = new CountDownLatch(rawTableNum);
        ThreadPoolExecutor executor = getExecutor();

        for (Entry<String, Set<String>> entry : rawSource.entrySet()) {
            String dbName = entry.getKey();
            Set<String> tableNames = entry.getValue();

            for (String tableName : tableNames) {
                executor.execute(() -> {
                    Set<ColumnStructure> columns = sourceColumnMap.get(dbName).get(tableName);
                    List<ColumnStructure> primaryColStructures = columns.stream().
                            filter(column -> StringUtils.equals(columnKey.PRI.name(), column.getColumnKey())).
                            sorted(Comparator.comparingLong(ColumnStructure::getOrdinalPosition)).
                            collect(Collectors.toList());

                    // 5.[OTHER_DIFF_REASON]: table缺少主键
                    try {
                        if (primaryColStructures == null || primaryColStructures.isEmpty()) {
                            MissPrimaryKeyInfo diffInfo = new MissPrimaryKeyInfo(dbName, tableName);
                            log.info("primaryColStructures is empty, dbName = {}, tableName = {}", dbName, tableName);
                            registry.registerDiffInfo(diffInfo);
                        } else {
                            // 6. [DIFF_FROM_RECORDS_CONTENT] 检查数据表内容差异
                            RecordContentDiffInfo diffInfo = new RecordContentDiffInfo(dbName, tableName);
                            diffInfo.setRecordsOnlyInSource(Lists.newArrayList());
                            diffInfo.setRecordsOnlyInTarget(Lists.newArrayList());
                            diffInfo.setSamePkDiffValues(Lists.newArrayList());

                            // db.table级别的record校验，每批次校验500条数据内容
                            int start = 0;
                            // diffInfo 不存储所有差异record，避免内存溢出
                            for (int i = 0; start == i * LIMIT && diffInfo.getRecordsOnlyInSource().size() < RECORD_STORAGE_LIMIT && diffInfo.getRecordsOnlyInTarget().size() < RECORD_STORAGE_LIMIT; i++) {
                                Map<PrimaryKeys, Record> sourceRecords = DBQueryService.querySourceRecords(sourceDataSource, dbName, tableName, primaryColStructures, start);
                                Map<PrimaryKeys, Record> targetRecords = DBQueryService.queryTargetRecords(targetDataSource, dbName, tableName, primaryColStructures, start, Lists.newArrayList(sourceRecords.values()));

                                MapDifference<PrimaryKeys, Record> recordMapDiff = Maps.difference(sourceRecords, targetRecords);

                                List<Record> recordOnlyInSource = Lists.newArrayList(recordMapDiff.entriesOnlyOnLeft().values());
                                List<Record> recordOnlyInTarget = Lists.newArrayList(recordMapDiff.entriesOnlyOnRight().values());
                                List<Pair<Record, Record>> sameKeyDiffValue = recordMapDiff.entriesDiffering().entrySet().stream().
                                        map(entry0 -> {
                                            MapDifference.ValueDifference<Record> valueDifference = entry0.getValue();
                                            return Pair.of(valueDifference.leftValue(), valueDifference.rightValue());
                                        }).collect(Collectors.toList());

                                if (sourceRecords.size() != targetRecords.size() || !recordOnlyInSource.isEmpty() || !recordOnlyInTarget.isEmpty() || !sameKeyDiffValue.isEmpty()) {
                                    diffInfo.getRecordsOnlyInSource().addAll(recordOnlyInSource);
                                    diffInfo.getRecordsOnlyInTarget().addAll(recordOnlyInTarget);
                                    diffInfo.getSamePkDiffValues().addAll(sameKeyDiffValue);
                                }

                                start += sourceRecords.size();
                            }

                            if (!diffInfo.getRecordsOnlyInTarget().isEmpty() || !diffInfo.getRecordsOnlyInSource().isEmpty() || !diffInfo.getSamePkDiffValues().isEmpty()) {
                                registry.registerDiffInfo(diffInfo);
                            }
                        }
                    } catch (Exception e) {
                        log.error("executor compare source records with target fail, dbName = {}, tableName = {}", dbName, tableName, e);
                    } finally {
                        latch.countDown();
                        log.info("latch countDown, current value is {}, dbName = {}, tableName = {}", latch.getCount(), dbName, tableName);
                        rawSource.computeIfAbsent(dbName, key -> Sets.newHashSet()).remove(tableName);
//                        if (latch.getCount() < 5) {
//                            log.info("remain rawSource = {}", rawSource);
//                        }
                    }
                });
            }
        }

        try {
            latch.await();
            executor.shutdown();
            registry.printResult();
        } catch (InterruptedException e) {
            log.error("[DataSyncValidator] countDownLatch await fail", e);
        }
        stopWatch.stop();
        log.info("[DIFF_FROM_RECORD_CONTENT]: the consuming is {} ms", stopWatch.getLastTaskTimeMillis());


        log.info(">>>>>>>>>>> [DataSyncValidator] core difference is {}", registry.getCoreDifferences());
    }

    private ThreadPoolExecutor getExecutor() {
        if (executor == null) {
            executor = new ThreadPoolExecutor(
                    config.getCorePoolSize(),
                    config.getMaximumPoolSize(),
                    config.getKeepAliveTime(),
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingDeque<>(),
                    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DataSyncValidator-%d").build(),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }
        return executor;
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
}
