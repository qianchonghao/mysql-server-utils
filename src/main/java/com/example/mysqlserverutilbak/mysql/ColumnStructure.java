package com.example.mysqlserverutilbak.mysql;

import com.google.common.base.Objects;
import lombok.Data;

/**
 * @Author qch
 * @Date 2022/8/14 5:54 下午
 */
@Data
public class ColumnStructure {
    private String tableSchema;
    private String tableName;
    private String columnName;
    private String columnDefault;
    private String isNullable;
    private String dataType;
    private long characterMaximumLength;
    private long characterOctetLength;
    private long numericPrecision;
    private long numericScale;
    private String dataTimePrecision;
    private String characterSetName;
    private String collationName;
    private String columnType;
    private String columnKey;
    private String privileges;
    private long ordinalPosition;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnStructure that = (ColumnStructure) o;
        return characterMaximumLength == that.characterMaximumLength && characterOctetLength == that.characterOctetLength && numericPrecision == that.numericPrecision && numericScale == that.numericScale && ordinalPosition == that.ordinalPosition && Objects.equal(tableSchema, that.tableSchema) && Objects.equal(tableName, that.tableName) && Objects.equal(columnName, that.columnName) && Objects.equal(columnDefault, that.columnDefault) && Objects.equal(isNullable, that.isNullable) && Objects.equal(dataType, that.dataType) && Objects.equal(dataTimePrecision, that.dataTimePrecision) && Objects.equal(characterSetName, that.characterSetName) && Objects.equal(collationName, that.collationName) && Objects.equal(columnType, that.columnType) && Objects.equal(columnKey, that.columnKey) && Objects.equal(privileges, that.privileges);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableSchema, tableName, columnName, columnDefault, isNullable, dataType, characterMaximumLength, characterOctetLength, numericPrecision, numericScale, dataTimePrecision, characterSetName, collationName, columnType, columnKey, privileges, ordinalPosition);
    }
}