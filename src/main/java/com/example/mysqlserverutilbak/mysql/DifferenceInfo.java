package com.example.mysqlserverutilbak.mysql;

import lombok.Data;

import java.util.List;
public abstract class DifferenceInfo {
    protected String dbName;
    protected String tableName;

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    abstract DiffType getDiffType();

    public String getKey(){
        return buildKey(getDbName(),getTableName());
    }

    public static String buildKey(String dbName, String tableName){
        return dbName + "_" + tableName;
    }

    /**
     * 差异的类型： 与source比较,
     * 1. 缺少db
     * 2. db相同，缺少table
     * 3. tableName相同, 缺少column(table structure不同)。
     * 4. table structure相同， table content sum 不同。
     */
    public enum DiffType {
        MISS_DATABASE,
        MISS_TABLE,
        DIFF_FROM_TABLE_STRUCTURE,
        DIFF_FROM_RECORD_NUM
    }

    // [MISS_DATABASE]
    public static class DBMissInfo extends DifferenceInfo {
        public static final String DB_MISS_TABLE = "*";
        public DBMissInfo(String dbName) {
            super.tableName = DB_MISS_TABLE;
            super.dbName = dbName;
        }

        @Override
        DiffType getDiffType() {
            return DiffType.MISS_DATABASE;
        }
    }

    // [MISS_TABLE]
    public static class TableMissInfo extends DifferenceInfo {
        public TableMissInfo(String dbName, String tableName) {
            super.tableName = tableName;
            super.dbName = dbName;
        }

        @Override
        DiffType getDiffType() {
            return DiffType.MISS_TABLE;
        }
    }

    // [DIFF_FROM_TABLE_STRUCTURE]
    public static class TableStructureDiffInfo extends DifferenceInfo {
        public TableStructureDiffInfo(String dbName, String tableName) {
            super.tableName = tableName;
            super.dbName = dbName;
        }

        @Override
        DiffType getDiffType() {
            return DiffType.DIFF_FROM_TABLE_STRUCTURE;
        }

        List<String> missColumn;
        List<String> redundantColumn;

        public List<String> getMissColumn() {
            return missColumn;
        }

        public void setMissColumn(List<String> missColumn) {
            this.missColumn = missColumn;
        }

        public List<String> getRedundantColumn() {
            return redundantColumn;
        }

        public void setRedundantColumn(List<String> redundantColumn) {
            this.redundantColumn = redundantColumn;
        }
    }

    // [DIFF_FROM_RECORD_NUM]
    public static class RecordDiffInfo extends DifferenceInfo {
        public RecordDiffInfo(String dbName, String tableName) {
            super.tableName = tableName;
            super.dbName = dbName;
        }

        @Override
        DiffType getDiffType() {
            return DiffType.DIFF_FROM_TABLE_STRUCTURE;
        }

        private long sourceNum;
        private long targetNum;

        public long getSourceNum() {
            return sourceNum;
        }

        public void setSourceNum(long sourceNum) {
            this.sourceNum = sourceNum;
        }

        public long getTargetNum() {
            return targetNum;
        }

        public void setTargetNum(long targetNum) {
            this.targetNum = targetNum;
        }
        /**
         * @leimo todo: 对比 record内容
         *      1. get ids in source
         *      2. batch query target records in source ids
         *      3. foreach compare target record with source's
         */
    }
}