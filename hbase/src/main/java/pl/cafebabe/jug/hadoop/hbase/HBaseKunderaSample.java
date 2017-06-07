package pl.cafebabe.jug.hadoop.hbase;

import pl.cafebabe.jug.hadoop.hbase.model.Odczyt;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.List;
import java.util.UUID;

public class HBaseKunderaSample {

    public static void main(String[] args) {
        EntityManagerFactory emf = Persistence.createEntityManagerFactory("hbase_pu");
        EntityManager em = emf.createEntityManager();

        Odczyt odczyt = new Odczyt();
        odczyt.setId(UUID.randomUUID().toString());
        odczyt.setNrl("nr 122");
        odczyt.setZuzycie("17");

        System.out.printf("Zapis odczytu %s\n", odczyt.getId());
        em.persist(odczyt);

        List<Odczyt> odczyty = em
                .createNamedQuery(Odczyt.ODCZYTY_DLA_LICZNIKA, Odczyt.class)
                .setParameter("nrl", "nr 122")
                .getResultList();
        System.out.printf("Liczba znalezionych wierszy: %d.\n", odczyty.size());

        odczyt = em.find(Odczyt.class, "1-17");
        if (odczyt != null) {
            System.out.printf("Pobrano odczyt - id: %s, zużycie: %s\n", odczyt.getId(), odczyt.getZuzycie());
        }

        em.close();
        emf.close();
    }
}