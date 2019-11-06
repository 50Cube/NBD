package com.rain.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class App 
{
    public static void main( String[] args )
    {
            final Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            final Session session = cluster.connect("rain");

        System.out.println("Cluster information:");
        System.out.println("Cluster name: " + cluster.getClusterName());
        System.out.println("Driver version: " + cluster.getDriverVersion());
        System.out.println("Cluster configuration: " + cluster.getConfiguration().toString());
        System.out.println("Cluster metadata: " + cluster.getMetadata().toString());
        System.out.println("Cluster metrics: " + cluster.getMetrics().toString());
        System.out.println();

        DatabaseOperations db = new DatabaseOperations();
        //db.getTable(session);
        db.getTableLimited(session, 5);
        System.out.println();
        //db.insert(session, "('2019-11-02', 0, 12, 3, False, 'Zgierz', 'Europe', 9, 1010)");
        System.out.println();
        db.getTableWhereCity(session, "Zgierz");
        System.out.println();
//        db.deleteByCity(session, "Zgierz");
//        System.out.println();
//        db.getTableWhereCity(session, "Zgierz");
        db.updateCityByDate(session, "2019-11-02", "Lodz", "Europe");
        System.out.println();
        db.getTableWhereCity(session, "Lodz");
        System.out.println();
        db.getTableWhereDate(session, "2019-11-02");
        db.getTableWhereDateToJSON(session, "2019-11-02");

        //db.insertFromJSON(session, "insertJSON.json");
        System.out.println();
        db.getTableWhereDate(session, "2019-11-05");
        db.getTableWhereCity(session, "Pabianice");
        System.out.println();
        db.getTableWherePRCPisMore(session, 3.0f);
        System.out.println();
        db.getTableWhereTMAXisMore(session, 98);
        System.out.println();
        db.getTableWhereTINisLess(session, 5);


        cluster.close();
        session.close();
    }
}
