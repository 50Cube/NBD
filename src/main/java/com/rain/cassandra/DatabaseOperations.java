package com.rain.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class DatabaseOperations
{
    private ResultSet results;
    public DatabaseOperations() {}

    private void printTable(ResultSet results)
    {
        for (Row row : results)
        {
            System.out.format("%s %.2f %d %d %s %s %s %d %d\n", row.getDate("DATE"), row.getFloat("PRCP"), row.getInt("TMAX"), row.getInt("TMIN"),
                    row.getBool("RAIN"), row.getString("CITY"), row.getString("CONTINENT"), row.getInt("TAMP"), row.getInt("PRESSURE"));
        }
    }

    public void getTable(Session session)
    {
        results = session.execute("select * from Rain");
        printTable(results);
    }

    public void getTableLimited(Session session, int limit)
    {
        results = session.execute("select * from Rain limit " + limit);
        printTable(results);
    }

    public void getTableWhereCity(Session session, String city)
    {
        results = session.execute("select * from Rain where CITY=" + city + " allow filtering");
        printTable(results);
    }

    public void insert(Session session, String row)
    {
        results = session.execute("insert into Rain (DATE, PRCP, TMAX, TMIN, RAIN, CITY, CONTINENT, TAMP, PRESSURE) " +
                "values " + row);
        printTable(results);
    }

    public void deleteByCity(Session session, String city)
    {
        results = session.execute("delete from Rain where CITY=" + city + " allow filtering");
        printTable(results);
    }
}
