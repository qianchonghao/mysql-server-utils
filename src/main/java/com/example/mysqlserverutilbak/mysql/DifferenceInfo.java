package com.example.mysqlserverutilbak.mysql;

import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

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

    /**
     * 差异的类型： 与source比较,
     * 1. 缺少db
     * 2. db相同，缺少table
     * 3. tableName相同, 缺少column(table structure不同)。
     * 4. table structure相同， table content sum 不同。
     * 5. table record 不同
     */
    public enum DiffType {
        MISS_DATABASE,
        MISS_TABLE,
        DIFF_FROM_TABLE_STRUCTURE,
        DIFF_FROM_RECORD_NUM,
        DIFF_FROM_RECORD_CONTENT,
        MISS_PRIMARY_KEY,
        OUT_OF_LIMIT;
//        abstract
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

        boolean outOfLimit = false;

        public boolean getOutOfLimit() {
            return outOfLimit;
        }

        public void setOutOfLimit(boolean outOfLimit) {
            this.outOfLimit = outOfLimit;
        }
    }
}
