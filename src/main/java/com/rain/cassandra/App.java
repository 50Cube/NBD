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

        DatabaseOperations db = new DatabaseOperations();
        db.getTable(session);
    }
}
