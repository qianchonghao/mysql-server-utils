package com.example.mysqlserverutilbak.mysql;

import lombok.Data;

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

    public static String buildKey(String dbName, String tableName){
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
        DIFF_FROM_RECORD_CONTENT
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
    public static class RecordDiffInfo extends DifferenceInfo {
        public RecordDiffInfo(String dbName, String tableName) {
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

    // 3. [DIFF_FROM_RECORD_CONTENT]
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
}
