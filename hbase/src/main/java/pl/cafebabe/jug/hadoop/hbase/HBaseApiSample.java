package pl.cafebabe.jug.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseApiSample {

    private static final byte[] CF = Bytes.toBytes("CF");
    private static final byte[] NRL = Bytes.toBytes("NRL");
    private static final byte[] ZUZYCIE = Bytes.toBytes("ZUZYCIE");

    // https://community.hortonworks.com/articles/4091/hbase-client-application-best-practices.html
    public static void main(String[] args) throws Exception {
        // konfiguracja z pliku hbase-site.xml
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Table table = connection.getTable(TableName.valueOf("ODCZYTY"));

            // wstawienie lub aktualizacja danych
            Put put = new Put(Bytes.toBytes("3-17"));
            put
                    .addColumn(CF, NRL, Bytes.toBytes("nr 17"))
                    .addColumn(CF, ZUZYCIE, Bytes.toBytes("256"));
            table.put(put);

            // pobranie danych
            Get get = new Get(Bytes.toBytes("3-17"));
            Result result = table.get(get);
            System.out.println(result);
            System.out.printf("nrl: %s, zużycie: %s", Bytes.toString(result.getValue(CF, NRL)), Bytes.toInt(result.getValue(CF, NRL)));
        }
        // get 'ODCZYTY', '3-17'
    }
}