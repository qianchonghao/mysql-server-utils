package com.example.mysqlserverutilbak.mysql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Objects;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static com.example.mysqlserverutilbak.mysql.ColumnStructure.*;
/**
 * @Author qch
 * @Date 2022/8/15 7:45 下午
 */
public class Record {

    private List<Object> columnValues;
    private List<Pair<Object,ColumnStructure>> primaryColValues;

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append("    record PrimaryKey =").
                append(primaryColValues.stream().map(pair ->
                {
                    StringBuilder sb = new StringBuilder();
                    sb.append(pair.getRight().getColumnName()).append(": ").
                            append(pair.getLeft().toString()).append(", ");
                    return sb.toString();
                }).collect(Collectors.toList())).append(" \n    record content = ").
                append(columnValues.toString());

        return res.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        List<Object> columnValues0 = record.getColumnValues();

        return StringUtils.equals(columnValues0.toString(), columnValues.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnValues);
    }

    public List<Object> getColumnValues() {
        return columnValues;
    }

    public void setColumnValues(List<Object> columnValues) {
        this.columnValues = columnValues;
    }

    public List<Pair<Object, ColumnStructure>> getPrimaryColValues() {
        return primaryColValues;
    }

    public void setPrimaryColValues(List<Pair<Object, ColumnStructure>> primaryColValues) {
        this.primaryColValues = primaryColValues.stream().
                filter(p -> StringUtils.equals(p.getRight().getColumnKey(),columnKey.PRI.name())).
                sorted(Comparator.comparingLong(p -> p.getRight().getOrdinalPosition())).
                collect(Collectors.toList());
    }

    // @leimo todo : ColumnStructure.toString or Object.toString 返回对象引用地址？ 是否重写 toString()
    public static class PrimaryKeys{
        // pkName, pkValue
        private List<Pair<Object,ColumnStructure>> primaryColValues;

        public PrimaryKeys(List<Pair<Object, ColumnStructure>> primaryColValues) {
            this.primaryColValues = primaryColValues.stream().
                    filter(p -> StringUtils.equals(p.getRight().getColumnKey(),columnKey.PRI.name())).
                    sorted(Comparator.comparingLong(p -> p.getRight().getOrdinalPosition())).
                    collect(Collectors.toList());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrimaryKeys that = (PrimaryKeys) o;

            return StringUtils.equals(primaryColValues.toString(), that.primaryColValues.toString());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(primaryColValues);
        }

        public List<Pair<Object, ColumnStructure>> getPrimaryColValues() {
            return primaryColValues;
        }

    }
}
