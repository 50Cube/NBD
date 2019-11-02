package com.rain.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

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
        db.insert(session, "('2019-11-02', 0, 12, 3, False, 'Zgierz', 'Europe', 9, 1010)");
        System.out.println();
        db.getTableWhereCity(session, "'Zgierz'");
        System.out.println();
//        db.deleteByCity(session, "'Zgierz'");
//        System.out.println();
//        db.getTableWhereCity(session, "'Zgierz'");


        cluster.close();
        session.close();
    }
}
