package com.rain.cassandra;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import java.io.*;

public class JSONfile
{
    JSONObject jsonObject1;
    JSONObject jsonObject2;
    File file;
    FileWriter fileWriter;
    FileReader reader;
    JSONParser jsonParser;

    public JSONfile()
    {
    }

    public void saveToJSON(String date, float prcp, int tmax, int tmin, boolean rain, String city, String continent, int pressure)
    {
        jsonObject1 = new JSONObject();
        jsonObject1.put("DATE", date);
        jsonObject1.put("PRCP", prcp);
        jsonObject1.put("TMAX", tmin);
        jsonObject1.put("TMIN", tmax);
        jsonObject1.put("RAIN", rain);
        jsonObject1.put("CITY", city);
        jsonObject1.put("CONTINENT", continent);
        jsonObject1.put("TAMP", tmax - tmin);
        jsonObject1.put("PRESSURE", pressure);

        try
        {
            file = new File("databaseJSON.json");
            file.createNewFile();
            fileWriter = new FileWriter(file);
            fileWriter.write(jsonObject1.toJSONString());
            fileWriter.flush();
            fileWriter.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public String readFromJSON(String file)
    {
        StringBuilder jsonInsert = new StringBuilder("");
        try
        {
            jsonObject2 = new JSONObject();
            reader = new FileReader(file);
            jsonParser = new JSONParser();
            Object object = jsonParser.parse(reader);
            JSONObject jsonObject2 = (JSONObject) object;
            //System.out.println(jsonObject2.get("DATE"));
            jsonInsert.append("'{\"DATE\":\"");
            jsonInsert.append(jsonObject2.get("DATE"));
            jsonInsert.append("\" ,\"PRCP\":");
            jsonInsert.append(jsonObject2.get("PRCP"));
            jsonInsert.append(" ,\"TMAX\":");
            jsonInsert.append(jsonObject2.get("TMAX"));
            jsonInsert.append(" ,\"TMIN\":");
            jsonInsert.append(jsonObject2.get("TMIN"));
            jsonInsert.append(" ,\"RAIN\":");
            jsonInsert.append(jsonObject2.get("RAIN"));
            jsonInsert.append(" ,\"CITY\":\"");
            jsonInsert.append(jsonObject2.get("CITY"));
            jsonInsert.append("\" ,\"CONTINENT\":\"");
            jsonInsert.append(jsonObject2.get("CONTINENT"));
            jsonInsert.append("\" ,\"TAMP\":");
            jsonInsert.append(jsonObject2.get("TAMP"));
            jsonInsert.append(" ,\"PRESSURE\":");
            jsonInsert.append(jsonObject2.get("PRESSURE"));
            jsonInsert.append("}'");
        }
        catch(FileNotFoundException ex)
        {
            ex.printStackTrace();
        }
        catch(IOException io)
        {
            io.printStackTrace();
        }
        catch(ParseException pe)
        {
            pe.printStackTrace();
        }
        return jsonInsert.toString();
    }

}
