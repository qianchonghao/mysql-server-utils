package com.example.mysqlserverutilbak.mysql;

import com.example.mysqlserverutilbak.mysql.log.CoreMarker;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.io.Files;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
            coreDifferences.computeIfAbsent(diffType, key -> Maps.newTreeMap()).put(buildKey(dbName, tableName), differenceInfo);
        }

        differences.computeIfAbsent(diffType, (key) -> Maps.newTreeMap()).put(buildKey(dbName, tableName), differenceInfo);
    }

    public void printResult() {

        File file = new File("log/report.txt");
        if (file.exists()) {
            file.delete();
        }
        file.getParentFile().mkdirs();

        OutputStream os = null;
        try {
            file.createNewFile();
            os = new FileOutputStream(file);
        } catch (IOException e) {
            log.error("create report file fail", e);
        }

        writeReportFromDiffInfo(os, differences.computeIfAbsent(DiffType.MISS_DATABASE, key -> Maps.newHashMap()),
                diffInfo -> diffInfo.getDbName(), ">>>>>>>>>>>>>>>> 目标数据源缺少以下数据库： ");

        writeReportFromDiffInfo(os, differences.computeIfAbsent(DiffType.MISS_TABLE, key -> Maps.newHashMap()),
                diffInfo -> buildKey(diffInfo.getDbName(), diffInfo.getTableName()), ">>>>>>>>>>>>>>>> 目标数据源缺少以下数据表： ");

        writeReportFromDiffInfo(os, differences.computeIfAbsent(DiffType.DIFF_FROM_TABLE_STRUCTURE, key -> Maps.newHashMap()),
                diffInfo -> structureDiffToString((TableStructureDiffInfo) diffInfo), ">>>>>>>>>>>>>>>> 以下数据表结构存在差异： ");

        writeReportFromDiffInfo(os, coreDifferences.computeIfAbsent(DiffType.DIFF_FROM_RECORD_NUM, key -> Maps.newHashMap()),
                diffInfo -> countDiffToString((RecordCountDiffInfo) diffInfo), ">>>>>>>>>>>>>>>> 以下数据表记录总数存在差异： ");

        writeReportFromDiffInfo(os, coreDifferences.computeIfAbsent(DiffType.DIFF_FROM_RECORD_CONTENT, key -> Maps.newHashMap()),
                diffInfo -> contentDiffToString((RecordContentDiffInfo) diffInfo), ">>>>>>>>>>>>>>>> 以下【核心】数据表记录内容存在差异： ");

//        @leimo todo: 切换回 core
//        writeReportFromDiffInfo(os, coreDifferences.computeIfAbsent(DiffType.MISS_PRIMARY_KEY, key -> Maps.newHashMap()),
        writeReportFromDiffInfo(os, differences.computeIfAbsent(DiffType.MISS_PRIMARY_KEY, key -> Maps.newHashMap()),
                diffInfo -> buildKey(diffInfo.getDbName(), diffInfo.getTableName()), ">>>>>>>>>>>>>>>> 以下【核心】数据表不存在primary key，无法对比记录内容： ");

        try {
            os.close();
        } catch (IOException e) {
            log.error("outputStream close fail", e);
        }
    }

    private String countDiffToString(RecordCountDiffInfo countDiffInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(countDiffInfo.getDbName()).append(" 数据表名称 = ").append(countDiffInfo.getTableName()).
                append(" 源数据表记录总数 = ").append(countDiffInfo.getSourceTotalCount()).append(" 目标数据表记录总数 = ").append(countDiffInfo.getTargetTotalCount());
        log.info("countDiffToString string = {}",sb.toString());
        return sb.toString();
    }

    private String structureDiffToString(TableStructureDiffInfo structureDiffInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(structureDiffInfo.getDbName()).append(" 数据表名称 = ").append(structureDiffInfo.getTableName()).append("\n");

        sb.append(">>>>>>>>>>>>>>>> 仅source数据源存在的列结构 \n");
        structureDiffInfo.getColumnsOnlyInSource().stream().forEach((columnStructure) -> {
            sb.append(columnStructure.toString()).append("\n");
        });

        sb.append(">>>>>>>>>>>>>>>> 仅存在target数据源的列结构 \n");
        structureDiffInfo.getColumnsOnlyInTarget().stream().forEach((columnStructure) -> {
            sb.append(columnStructure.toString()).append("\n");
        });

        return sb.toString();
    }

    private String contentDiffToString(RecordContentDiffInfo contentDiffInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(contentDiffInfo.getDbName()).append(" 数据表名称 = ").append(contentDiffInfo.getTableName()).append("\n");

        sb.append(">>>>>>>>>>>>>>>> 仅source数据源存在的记录").append("\n");
        contentDiffInfo.getRecordsOnlyInSource().stream().forEach(record -> {
            sb.append("[source only]").append(record.toString()).append("\n");
        });

        sb.append(">>>>>>>>>>>>>>>> source和target同时存在但内容有差异的记录").append("\n");
        contentDiffInfo.getSamePkDiffValues().stream().forEach(pair -> {
            sb.append("[source]").append(pair.getLeft().toString()).append("\n").
                    append("[target]").append(pair.getRight().toString()).append("\n");

        });

        sb.append(">>>>>>>>>>>>>>>> 仅target数据源存在的记录").append("\n");
        contentDiffInfo.getRecordsOnlyInTarget().stream().forEach(record -> {
            sb.append("[target]").append(record.toString()).append("\n");

        });

        log.info("contentDiffToString string = {}",sb.toString());
        return sb.toString();
    }

    private void writeReportFromDiffInfo(OutputStream os, Map<String, DifferenceInfo> differenceInfoMap, Function<DifferenceInfo, String> lineProvider, String start) {
        List<String> content = Lists.newArrayList();
        content.add(start);

        content.addAll(Streams.mapWithIndex(differenceInfoMap.entrySet().stream(), (entry, index) -> {
            StringBuilder res = new StringBuilder();
            res.append(index+1).append(". ").append(lineProvider.apply(entry.getValue()));
            return res.toString();
        }).collect(Collectors.toList()));

        try {
            IOUtils.writeLines(content, "\n", os);
        } catch (IOException e) {
            log.error("write content fail", e);
        }
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
