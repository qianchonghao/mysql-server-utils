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
//        Map<String, String> map = new HashMap<>();
//        map.put("driverClassName", "com.mysql.cj.jdbc.Driver");
//        map.put("url", "jdbc:mysql://" + config.mysqlAddr + ":" + config.mysqlPort + "?&characterEncoding=UTF-8&useSSL=false&serverTimezone=GMT%2B8");
//        // @leimo todo : 链接 spring_demo的 db，执行 create db & create table ， 测试是否可行
//        map.put("url", "jdbc:mysql://" + config.mysqlAddr + ":" + config.mysqlPort +"/test"+ "?&characterEncoding=UTF-8&useSSL=false&serverTimezone=GMT%2B8");
//        map.put("username", config.mysqlUsername);
//        map.put("password", config.mysqlPassword);
//        map.put("initialSize", "2");
//        map.put("maxActive", "2");
//        map.put("maxWait", "60000");
//        map.put("timeBetweenEvictionRunsMillis", "60000");
//        map.put("minEvictableIdleTimeMillis", "300000");
//        map.put("validationQuery", "SELECT 1 FROM DUAL");
//        map.put("testWhileIdle", "true");

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
