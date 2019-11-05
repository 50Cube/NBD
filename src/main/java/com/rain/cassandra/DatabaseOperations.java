package com.rain.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class DatabaseOperations
{
    private ResultSet results;
    private JSONfile jsonFile;
    public DatabaseOperations() {
        jsonFile = new JSONfile();
    }


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

    public void getTableWhereDate(Session session, String date)
    {
        results = session.execute("select * from Rain where DATE='" + date + "' allow filtering");
        printTable(results);
    }

    public void getTableWhereDateToJSON(Session session, String date)
    {
        results = session.execute("select * from Rain where DATE='" + date + "' allow filtering");
        for (Row row : results)
        {
            jsonFile.saveToJSON(row.getDate("DATE").toString(),
                    row.getFloat("PRCP"),
                    row.getInt("TMAX"),
                    row.getInt("TMIN"),
                    row.getBool("RAIN"),
                    row.getString("CITY"),
                    row.getString("CONTINENT"),
                    row.getInt("PRESSURE"));
        }
    }

    public void getTableWhereCity(Session session, String city)
    {
        results = session.execute("select * from Rain where CITY='" + city + "' allow filtering");
        printTable(results);
    }

    public void insert(Session session, String row)
    {
        results = session.execute("insert into Rain (DATE, PRCP, TMAX, TMIN, RAIN, CITY, CONTINENT, TAMP, PRESSURE) " +
                "values " + row);
        printTable(results);
    }

    public void insertFromJSON(Session session, String file)
    {
        //System.out.println(jsonFile.readFromJSON(file));
        results = session.execute("insert into Rain json " + jsonFile.readFromJSON(file));
    }

    public void updateCityByDate(Session session, String date, String city, String continent)
    {
        results = session.execute("update Rain set CITY ='" + city + "', CONTINENT ='" + continent + "' where DATE = '" + date + "'");
        printTable(results);
    }

    public void deleteByDate(Session session, String date)
    {
        results = session.execute("delete from Rain where DATE='" + date + "'");
        printTable(results);
    }

    // DO NAPRAWIENIA
    public void deleteByCity(Session session, String city)
    {
        results = session.execute("delete from Rain where CITY='" + city + "' allow filtering");
        printTable(results);
    }
}
