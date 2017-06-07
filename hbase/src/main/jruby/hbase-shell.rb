# utworzenie tabeli
create 'ODCZYTY', 'CF'
create 'ODCZYTY', { NAME => 'CF', VERSIONS => 3 }
list

# wstawienie danych
for i in 1..1000
    put 'ODCZYTY', "1-#{i}", 'CF:NRL', "nr #{i}"
    put 'ODCZYTY', "1-#{i}", 'CF:ZUZYCIE', rand(200)
end

for i in 1..1000
    put 'ODCZYTY', "2-#{i}", 'CF:NRL', "nr #{i}"
    put 'ODCZYTY', "2-#{i}", 'CF:ZUZYCIE', rand(200)
end

# pobranie i aktualizacja danych
get 'ODCZYTY', '1-17'
put 'ODCZYTY', '1-17', 'CF:ZUZYCIE', 99
get 'ODCZYTY', '1-17'
get 'ODCZYTY', '1-17', { COLUMN => ['CF:NRL', 'CF:ZUZYCIE'], VERSIONS => 10}
get 'ODCZYTY', '2-17'

scan 'ODCZYTY'

# https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/package-summary.html
# https://www.cloudera.com/documentation/enterprise/5-9-x/topics/admin_hbase_filtering.html
scan 'ODCZYTY', { FILTER => "ValueFilter(=, 'binary:nr 17')" }
scan 'ODCZYTY', { FILTER => "PrefixFilter('1-')" }

# usunięcie tabeli
disable 'ODCZYTY'
drop 'ODCZYTY'
