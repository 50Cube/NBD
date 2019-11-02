package com.rain.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class DatabaseOperations
{
    public DatabaseOperations() {}

    public void printTable(ResultSet results)
    {
        for (Row row : results)
        {
            System.out.format("%s %.2f %d %d %s %s %s %d %d\n", row.getDate("DATE"), row.getFloat("PRCP"), row.getInt("TMAX"), row.getInt("TMIN"),
                    row.getBool("RAIN"), row.getString("CITY"), row.getString("CONTINENT"), row.getInt("TAMP"), row.getInt("PRESSURE"));
        }
    }

    public void getTable(Session session)
    {
        ResultSet results = session.execute("select * from Rain");
        printTable(results);
    }
}
