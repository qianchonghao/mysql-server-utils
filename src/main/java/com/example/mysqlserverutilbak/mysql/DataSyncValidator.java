package com.example.mysqlserverutilbak.mysql;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
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

    @Autowired
    private DataSource sourceDataSource;

    @Autowired
    private DataSource targetDataSource;


    private Set<String> sourceDBNames = Sets.newHashSet();
    private Set<String> targetDBNames = Sets.newHashSet();

    @Override
    public void afterPropertiesSet() {
        CheckResult res = new CheckResult();

        targetDBNames = getAllDBInDataSource(targetDataSource);
        sourceDBNames = getAllDBInDataSource(sourceDataSource);
        // rawSource 标记未被处理的 db.table
        Map<String, List<String>> rawSource = sourceDBNames.stream().collect(Collectors.toMap(
                dbName -> dbName,
                dbName -> Lists.newArrayList()
        ));


        // 1. [MISS_DATABASE]： 构建差集，关注 source具备，但是target不具备的 datasource。
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
        for (String dbName : rawSource.keySet()) {
            Set<String> sourceTables = getTables(sourceDataSource, dbName);
            Set<String> targetTables = getTables(targetDataSource, dbName);
            SetView<String> tableDifference = Sets.difference(sourceTables, targetTables);

            //rawTables 标记 dbNames下未处理的table
            List<String> rawTables = Lists.newArrayList();
            rawTables.addAll(sourceTables);

            if (!tableDifference.isEmpty()) {
                rawTables.removeAll(tableDifference);
                for (String tableName : tableDifference) {
                    res.registerDiffInfo(new TableMissInfo(dbName, tableName));
                }
            }

            if (rawTables.isEmpty()) {
                rawSource.remove(dbName);
            } else {
                rawSource.put(dbName, rawTables);
            }
        }


        log.info(">>>>>>>>>>> [DataSyncValidator] result is {}",res);
//        // 3. [DIFF_FROM_TABLE_STRUCTURE]: db.table为key，对比source, target的表结构
//        for (Map.Entry<String, List<String>> entry : rawSource.entrySet()) {
//            String dbName = entry.getKey();
//            List<String> tables = entry.getValue();
//            // @leimo todo:
//            //  information_schema.INNODB_TABLES: 查询 tableName -> tableId
//            //  information_schema.INNODB_COLUMNS: 查询 tableId -> columns
//
//        }
    }

    private Set<String> getTables(DataSource dataSource, String dbName) {
        Set<String> tableNames = executeQuery(dataSource, SHOW_TABLES + dbName, (rs) -> {
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
