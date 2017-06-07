package pl.cafebabe.jug.hadoop.hive;

import static pl.cafebabe.jug.hadoop.commons.SQLUtils.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class HiveJdbcSample {

    public static void main(String[] args) throws Exception {
        // Class.forName("org.apache.hive.jdbc.HiveDriver");
        try (Connection connection = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/movielens", "hive", "");
             Statement stmt = connection.createStatement()) {

            print(stmt.executeQuery("SHOW TABLES"));
            print(stmt.executeQuery("SELECT m.title, count(*) as liczba, avg(r.rating) AS ocena\n"
                    + "FROM movies_he m, ratings_he r\n"
                    + "WHERE m.movie_id = r.movie_id\n"
                    + "GROUP BY m.title\n"
                    + "HAVING liczba > 100\n"
                    + "ORDER BY ocena desc\n"
                    + "LIMIT 10"));
        }
    }
}
