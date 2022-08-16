package com.example.mysqlserverutilbak.mysql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Objects;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * @Author qch
 * @Date 2022/8/15 7:45 下午
 */
@Data
public class Record {

    private List<Object> columnValues;
    private List<Pair<Object,ColumnStructure>> primaryColValues;

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
}
