package com.example.mysqlserverutilbak.mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
@Slf4j
public abstract class DifferenceInfo {
    private String dbName;
    private String tableName;

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    abstract DiffType getDiffType();

    public static String buildKey(String dbName, String tableName) {
        return dbName + "." + tableName;
    }

    // 差异项 -> String
    public abstract String toString();

    /**
     * 差异的类型： 与source比较,
     * 1. 缺少db
     * 2. db相同，缺少table
     * 3. tableName相同, 缺少column(table structure不同)。
     * 4. table structure相同， table content sum 不同。
     * 5. table record 不同
     */
    public enum DiffType {
        MISS_DATABASE("目标数据源缺少以下数据库"),
        MISS_TABLE("目标数据源缺少以下数据表"),
        DIFF_FROM_TABLE_STRUCTURE("以下数据表结构存在差异"),
        DIFF_FROM_RECORD_NUM("以下数据表记录总数存在差异"),
        DIFF_FROM_RECORD_CONTENT("以下数据表记录内容存在差异"),
        MISS_PRIMARY_KEY("以下数据表不存在primary key，无法对比记录内容"),
        OUT_OF_LIMIT("以下数据表的记录总数超过最大限制，并未对比全部记录内容");

        private String titleContent;

        DiffType(String titleContent) {
            this.titleContent = titleContent;
        }

        public String getTitle(String prefix){
            String diffTitleFormat = ">>>>>>>>>>>>>>>> " + prefix + " %s <<<<<<<<<<<<<<<<<<";
            return String.format(diffTitleFormat,titleContent);
        }
    }

    // 1. [MISS_DATABASE]
    public static class DBMissInfo extends DifferenceInfo {
        public static final String DB_MISS_TABLE = "*";

        public DBMissInfo(String dbName) {
            super.setDbName(dbName);
            super.setTableName(DB_MISS_TABLE);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.MISS_DATABASE;
        }

        @Override
        public String toString() {
            return getDbName();
        }
    }

    // 2. [MISS_TABLE]
    public static class TableMissInfo extends DifferenceInfo {
        public TableMissInfo(String dbName, String tableName) {
            super.setDbName(dbName);
            super.setTableName(tableName);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.MISS_TABLE;
        }

        @Override
        public String toString() {
            return buildKey(getDbName(), getTableName());
        }
    }

    // 3. [DIFF_FROM_TABLE_STRUCTURE]
    public static class TableStructureDiffInfo extends DifferenceInfo {
        public TableStructureDiffInfo(String dbName, String tableName) {
            super.setDbName(dbName);
            super.setTableName(tableName);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.DIFF_FROM_TABLE_STRUCTURE;
        }

        List<ColumnStructure> columnsOnlyInSource;
        List<ColumnStructure> columnsOnlyInTarget;


        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(getDbName()).append(" 数据表名称 = ").append(getTableName()).append("\n");

            sb.append(">>>>>>>>>>>>>>>> 仅source数据源存在的列结构 \n");
            getColumnsOnlyInSource().stream().forEach((columnStructure) -> {
                sb.append(columnStructure.toString()).append("\n");
            });

            sb.append(">>>>>>>>>>>>>>>> 仅存在target数据源的列结构 \n");
            getColumnsOnlyInTarget().stream().forEach((columnStructure) -> {
                sb.append(columnStructure.toString()).append("\n");
            });

            return sb.toString();
        }

        public List<ColumnStructure> getColumnsOnlyInSource() {
            return columnsOnlyInSource;
        }

        public void setColumnsOnlyInSource(List<ColumnStructure> columnsOnlyInSource) {
            this.columnsOnlyInSource = columnsOnlyInSource;
        }

        public List<ColumnStructure> getColumnsOnlyInTarget() {
            return columnsOnlyInTarget;
        }

        public void setColumnsOnlyInTarget(List<ColumnStructure> columnsOnlyInTarget) {
            this.columnsOnlyInTarget = columnsOnlyInTarget;
        }
    }

    // 4. [DIFF_FROM_RECORD_NUM]
    public static class RecordCountDiffInfo extends DifferenceInfo {
        public RecordCountDiffInfo(String dbName, String tableName) {
            super.setDbName(dbName);
            super.setTableName(tableName);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.DIFF_FROM_RECORD_NUM;
        }

        private long sourceTotalCount;
        private long targetTotalCount;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(getDbName()).append(" 数据表名称 = ").append(getTableName()).
                    append(" 源数据表记录总数 = ").append(getSourceTotalCount()).append(" 目标数据表记录总数 = ").append(getTargetTotalCount());
            log.info("countDiffToString string = {}", sb.toString());
            return sb.toString();
        }

        public long getSourceTotalCount() {
            return sourceTotalCount;
        }

        public void setSourceTotalCount(long sourceTotalCount) {
            this.sourceTotalCount = sourceTotalCount;
        }

        public long getTargetTotalCount() {
            return targetTotalCount;
        }

        public void setTargetTotalCount(long targetTotalCount) {
            this.targetTotalCount = targetTotalCount;
        }
    }

    // 5. [DIFF_FROM_RECORD_CONTENT]
    public static class RecordContentDiffInfo extends DifferenceInfo {
        public RecordContentDiffInfo(String dbName, String tableName) {
            super.setDbName(dbName);
            super.setTableName(tableName);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.DIFF_FROM_RECORD_CONTENT;
        }

        List<Record> recordsOnlyInSource;
        List<Record> recordsOnlyInTarget;
        List<Pair<Record, Record>> samePkDiffValues;


        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(">>>>>>>>>>>>>>>> 数据库名称 = ").append(getDbName()).append(" 数据表名称 = ").append(getTableName()).append("\n");

            sb.append(">>>>>>>>>>>>>>>> 仅source数据源存在的记录").append("\n");
            getRecordsOnlyInSource().stream().forEach(record -> {
                sb.append("[source only]").append(record.toString()).append("\n");
            });

            sb.append(">>>>>>>>>>>>>>>> source和target同时存在但内容有差异的记录").append("\n");
            getSamePkDiffValues().stream().forEach(pair -> {
                sb.append("[source]").append(pair.getLeft().toString()).append("\n").
                        append("[target]").append(pair.getRight().toString()).append("\n");

            });

            sb.append(">>>>>>>>>>>>>>>> 仅target数据源存在的记录").append("\n");
            getRecordsOnlyInTarget().stream().forEach(record -> {
                sb.append("[target]").append(record.toString()).append("\n");

            });

            log.info("contentDiffToString string = {}", sb.toString());
            return sb.toString();
        }

        public List<Pair<Record, Record>> getSamePkDiffValues() {
            return samePkDiffValues;
        }

        public void setSamePkDiffValues(List<Pair<Record, Record>> samePkDiffValues) {
            this.samePkDiffValues = samePkDiffValues;
        }

        public List<Record> getRecordsOnlyInSource() {
            return recordsOnlyInSource;
        }

        public void setRecordsOnlyInSource(List<Record> recordsOnlyInSource) {
            this.recordsOnlyInSource = recordsOnlyInSource;
        }

        public List<Record> getRecordsOnlyInTarget() {
            return recordsOnlyInTarget;
        }

        public void setRecordsOnlyInTarget(List<Record> recordsOnlyInTarget) {
            this.recordsOnlyInTarget = recordsOnlyInTarget;
        }
    }

    // 6. [MISS_PRIMARY_KEY]
    public static class MissPrimaryKeyInfo extends DifferenceInfo {

        public MissPrimaryKeyInfo(String dbName, String tableName) {
            super.setDbName(dbName);
            super.setTableName(tableName);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.MISS_PRIMARY_KEY;
        }

        @Override
        public String toString() {
            return buildKey(getDbName(), getTableName());
        }
    }

    // 7. [OUT_OF_LIMIT]
    public static class OutOfLimitInfo extends DifferenceInfo{
        // 每次循环对比500条数据。1张数据表默认循环20次，对比1w条数据。超出1w条则注册OutOfLimitInfo
        public static int RECORD_COMPARE_LIMIT = 20;

        public OutOfLimitInfo(String dbName, String tableName) {
            super.setDbName(dbName);
            super.setTableName(tableName);
        }

        @Override
        DiffType getDiffType() {
            return DiffType.OUT_OF_LIMIT;
        }

        @Override
        public String toString() {
            return buildKey(getDbName(), getTableName());
        }
    }
}
