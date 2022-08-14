package com.example.mysqlserverutilbak.mysql.util;

import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;
@Slf4j
public class SqlUtils {
    public static void executeUpdate(DataSource ds, String sql, Object... args) {
        Connection conn = null;
        PreparedStatement preparedStatement = null;

        try {
            conn = ds.getConnection();
            preparedStatement = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                preparedStatement.setObject(i, args[i]);
            }
            preparedStatement.execute();
        } catch (SQLException e) {
            System.out.println("sql execute fail");
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                System.out.println("connection close fail");
            }
        }
    }

    public static <T> T executeQuery(DataSource ds, String sql, Function<ResultSet, T> func, Object... args) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = ds.getConnection();
            ps = conn.prepareStatement(sql);
            for (int i = 0; i < args.length; i++) {
                ps.setObject(i+1, args[i]);
            }
            rs = ps.executeQuery();
            return func.apply(rs);
        } catch (SQLException e) {
            log.error("execute sql query fail",e);
            return null;
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
                System.out.println("connection close fail");
            }
        }
    }

//    public static ResultSet executeQuery(DataSource ds, String sql, Object... args) {
//        return executeQuery(ds, sql, (rs) -> {
//            return rs;
//        }, args);
//    }
}
