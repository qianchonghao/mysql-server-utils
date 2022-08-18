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

import java.io.*;
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

    public void printCoreResult() {
        printResult(coreDifferences, ResultType.CORE);
    }

    public void printAllResult() {
        printResult(differences, ResultType.ALL);
    }

    private void printResult(Map<DiffType, Map<String, DifferenceInfo>> differences0, ResultType resultType) {
        String prefix = resultType.getPrefix();
        String filePrefix = resultType.getName();

        File file = new File(String.format("log/%s-report.txt", filePrefix));
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

        String diffTitleFormat = ">>>>>>>>>>>>>>>> " + prefix + " %s <<<<<<<<<<<<<<<<<<";

        if (resultType == ResultType.CORE) {
            Map<String, DifferenceInfo> coreDiff = differences0.entrySet().stream().
                    filter(entry -> !entry.getValue().isEmpty()).
                    map(entry -> entry.getValue()).findFirst().orElse(null);
            String result = coreDiff == null ?
                    String.format(diffTitleFormat, "source数据源和target数据源，内容一致，数据同步成功\n") :
                    String.format(diffTitleFormat, "source数据源和target数据源，内容不一致，数据同步失败\n");
            try {
                IOUtils.write(result, os);
            } catch (IOException e) {
                log.error("core result write fail", e);
            }
        }

//        differences0.entrySet().stream().forEach((entry)->{
//            OutputStream os = null;
//            try {
//                os = new FileOutputStream(file);
//            } catch (FileNotFoundException e) {
//                log.error("create report file fail", e);
//            }
//
//            writeReportFromDiffInfo(os,entry.getValue(),diffInfo->diffInfo.getDbName(),String.format(diffTitleFormat,"aa"));
//        });

        writeReportFromDiffInfo(os, differences0.computeIfAbsent(DiffType.MISS_DATABASE, key -> Maps.newHashMap()),
                diffInfo -> diffInfo.getDbName(), String.format(diffTitleFormat, "目标数据源缺少以下数据库"));

        writeReportFromDiffInfo(os, differences0.computeIfAbsent(DiffType.MISS_TABLE, key -> Maps.newHashMap()),
                diffInfo -> buildKey(diffInfo.getDbName(), diffInfo.getTableName()), String.format(diffTitleFormat, "目标数据源缺少以下数据表"));

        writeReportFromDiffInfo(os, differences0.computeIfAbsent(DiffType.DIFF_FROM_TABLE_STRUCTURE, key -> Maps.newHashMap()),
                diffInfo -> structureDiffToString((TableStructureDiffInfo) diffInfo), String.format(diffTitleFormat, "以下数据表结构存在差异"));

        writeReportFromDiffInfo(os, differences0.computeIfAbsent(DiffType.DIFF_FROM_RECORD_NUM, key -> Maps.newHashMap()),
                diffInfo -> countDiffToString((RecordCountDiffInfo) diffInfo), String.format(diffTitleFormat, "以下数据表记录总数存在差异"));

        writeReportFromDiffInfo(os, differences0.computeIfAbsent(DiffType.DIFF_FROM_RECORD_CONTENT, key -> Maps.newHashMap()),
                diffInfo -> contentDiffToString((RecordContentDiffInfo) diffInfo), String.format(diffTitleFormat, "以下数据表记录内容存在差异"));

        writeReportFromDiffInfo(os, differences0.computeIfAbsent(DiffType.MISS_PRIMARY_KEY, key -> Maps.newHashMap()),
                diffInfo -> buildKey(diffInfo.getDbName(), diffInfo.getTableName()), String.format(diffTitleFormat, "以下数据表不存在primary key，无法对比记录内容"));


        try {
            log.info(IOUtils.toString(file.toURI()));
            os.close();
        } catch (IOException e) {
            log.error("outputStream close fail", e);
        }
    }

    private String countDiffToString(RecordCountDiffInfo countDiffInfo) {
        StringBuilder sb = new StringBuilder();
        sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(countDiffInfo.getDbName()).append(" 数据表名称 = ").append(countDiffInfo.getTableName()).
                append(" 源数据表记录总数 = ").append(countDiffInfo.getSourceTotalCount()).append(" 目标数据表记录总数 = ").append(countDiffInfo.getTargetTotalCount());
        log.info("countDiffToString string = {}", sb.toString());
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

        log.info("contentDiffToString string = {}", sb.toString());
        return sb.toString();
    }

    private void writeReportFromDiffInfo(OutputStream os, Map<String, DifferenceInfo> differenceInfoMap, Function<DifferenceInfo, String> lineProvider, String start) {
        List<String> content = Lists.newArrayList();
        content.add(start);

        content.addAll(Streams.mapWithIndex(differenceInfoMap.entrySet().stream(), (entry, index) -> {
            StringBuilder res = new StringBuilder();
            res.append(index + 1).append(". ").append(lineProvider.apply(entry.getValue()));
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

    private enum ResultType {
        CORE("[核心数据源差异]", "core"),
        ALL("[全部数据源差异]", "all");

        private final String prefix;
        private final String name;

        ResultType(String prefix, String name) {
            this.prefix = prefix;
            this.name = name;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getName() {
            return name;
        }
    }
}
