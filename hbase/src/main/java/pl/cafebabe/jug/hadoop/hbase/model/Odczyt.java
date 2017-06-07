package pl.cafebabe.jug.hadoop.hbase.model;

import javax.persistence.*;

@Entity
@Table(name = "ODCZYTY")
// https://groups.google.com/forum/#!topic/kundera-discuss/RUbHw0Fd6s4
@SecondaryTable(name = "CF")
@NamedQuery(
        name = Odczyt.ODCZYTY_DLA_LICZNIKA,
        query = "SELECT o FROM Odczyt o WHERE o.nrl = :nrl")
public class Odczyt {

    public static final String ODCZYTY_DLA_LICZNIKA = "Odczyt.odczytyDlaLicznika";

    @Id
    private String id;

    @Column(name = "NRL", table = "CF")
    private String nrl;

    @Column(name = "ZUZYCIE", table = "CF")
    private String zuzycie;

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getNrl() {
        return nrl;
    }

    public void setNrl(String nrl) {
        this.nrl = nrl;
    }

    public String getZuzycie() {
        return zuzycie;
    }

    public void setZuzycie(String zuzycie) {
        this.zuzycie = zuzycie;
    }

}