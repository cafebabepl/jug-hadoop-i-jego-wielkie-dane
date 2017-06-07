package pl.cafebabe.jug.hadoop.commons;

import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class SQLUtils {

    public static void print(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        // nazwy kolumn
        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            System.out.printf("%s|", StringUtils.center(rsmd.getColumnLabel(i), 30));
        }
        System.out.println();
        // wiersze
        while (rs.next()) {
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                System.out.printf("%-30s|", StringUtils.abbreviate(rs.getString(i), 30));
            }
            System.out.println();
        }
        System.out.println();
    }
}