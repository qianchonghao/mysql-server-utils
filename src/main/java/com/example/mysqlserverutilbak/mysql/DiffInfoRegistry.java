package com.example.mysqlserverutilbak.mysql;

import com.example.mysqlserverutilbak.mysql.log.CoreMarker;
import com.google.common.collect.ImmutableSet;
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

    private static final Set<DiffType> EXCLUDE_CORE_DIFF = ImmutableSet.of(DiffType.OUT_OF_LIMIT);

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

        String diffTitleFormat = ">>>>>>>>>>>>>>>> " + prefix + " %s <<<<<<<<<<<<<<<<<< \n";

        if (resultType == ResultType.CORE) {
            Map<String, DifferenceInfo> coreDiff = differences0.entrySet().stream().
                    filter(entry -> !entry.getValue().isEmpty() && !EXCLUDE_CORE_DIFF.contains(entry.getKey())).
                    map(entry -> entry.getValue()).findFirst().orElse(null);
            String result = coreDiff == null ?
                    String.format(diffTitleFormat, "source数据源和target数据源，内容一致，数据同步成功") :
                    String.format(diffTitleFormat, "source数据源和target数据源，内容不一致，数据同步失败");
            try {
                IOUtils.write(result, os);
            } catch (IOException e) {
                log.error("core result write fail", e);
            }
        }

        OutputStream finalOs = os;
        differences0.entrySet().stream().forEach((entry) -> {
            writeReportFromDiffInfo(finalOs, differences0, entry.getKey(), prefix);
        });

        try {
            log.info(IOUtils.toString(file.toURI()));
            os.close();
        } catch (IOException e) {
            log.error("outputStream close fail", e);
        }
    }

    private void writeReportFromDiffInfo(OutputStream os, Map<DiffType, Map<String, DifferenceInfo>> differences0, DiffType diffType, String prefix) {
        Map<String, DifferenceInfo> differenceInfoMap = differences0.get(diffType);
        List<String> content = Lists.newArrayList();
        content.add(diffType.getTitle(prefix));

        content.addAll(Streams.mapWithIndex(differenceInfoMap.entrySet().stream(), (entry, index) -> {
            StringBuilder res = new StringBuilder();
            res.append(index + 1).append(". ").append(entry.getValue().toString());
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
