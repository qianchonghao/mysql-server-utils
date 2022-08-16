package com.example.mysqlserverutilbak.mysql;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.fastjson.JSONObject;
import com.google.common.io.CharStreams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

@Configuration

@Slf4j
public class MysqlConfiguration {
    @Bean("sourceDataSource")
    public DataSource getSourceDataSource() {
        return getDataSource(getConfigChild("source"));
    }

    @Bean("targetDataSource")
    public DataSource getTargetDataSource() {
        return getDataSource(getConfigChild("target"));
    }

    private DataSource getDataSource(JSONObject conf) {
        DataSource dataSource = null;
        try {
            dataSource = DruidDataSourceFactory.createDataSource(conf);
        } catch (Exception e) {
            log.error("[buildDataSource] create datasource  fail", e);
        }

        return dataSource;
    }

    @Bean("validateConfig")
    public ValidateConfig getValidateConfig(){
        JSONObject json = getConfigChild("config");
        ValidateConfig config = json.toJavaObject(ValidateConfig.class);
        return config;
    }

    private JSONObject getConfigChild(String childKey) {
        // @leimo todo : 可更换 conf获取的途径
        JSONObject res = null;
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("conf.json");
        try {
            String jsonStr = CharStreams.toString(new InputStreamReader(is));
            res = JSONObject.parseObject(jsonStr).getJSONObject(childKey);
        } catch (IOException e) {
            log.error("[MysqlConfiguration]: read conf fail", e);
        }
        return res;
    }

}
