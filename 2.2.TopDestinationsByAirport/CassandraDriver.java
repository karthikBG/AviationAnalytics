mport java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import javax.xml.soap.Text;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;


public class CassandraDriver {

    public static void main(String[] args) throws FileNotFoundException {
        // TODO Auto-generated method stub
        
        Cluster cluster;
        Session session;
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();
        session.execute("USE tp");
        
        String query = "CREATE TABLE topdestinationbyairport(airport_name text, destination_name text, performance float, PRIMARY KEY (airport_name, performance, destination_name)) WITH CLUSTERING ORDER BY (performance ASC);";
        session.execute(query);
        
        
        
        Scanner input = new Scanner(new File("ExportToCassandraAvg.txt"));
        
        while(input.hasNextLine()) {
            String line = input.nextLine();
            String[] split = line.split("   ");
            String airportX = split[0].trim();
            String[] split1 = split[1].split(" ");
            
            for(int i = 0 ; i < split1.length ; i++) {
                String[] split3 = split1[i].split("_");
                String airlineX = split3[0];
                float performanceX = Float.parseFloat(split3[1]);
                
                session.execute("INSERT INTO tp.topdestinationbyairport(airport_name, destination_name, performance) VALUES (?,?,?)", airportX, airlineX, performanceX);
            }
        }
        
        ResultSet results = session.execute("SELECT * FROM topdestinationbyairport WHERE airport_name='CMI' LIMIT 10;");
        for (Row row : results) {
            System.out.println(row.getString("airport_name") + "    " + row.getString("destination_name")+"_"+row.getFloat("performance"));
        }
        
        results = session.execute("SELECT * FROM topdestinationbyairport WHERE airport_name='BWI' LIMIT 10;");
        for (Row row : results) {
            System.out.println(row.getString("airport_name") + "    " + row.getString("destination_name")+"_"+row.getFloat("performance"));
        }
        results = session.execute("SELECT * FROM topdestinationbyairport WHERE airport_name='MIA' LIMIT 10;");
        for (Row row : results) {
            System.out.println(row.getString("airport_name") + "    " + row.getString("destination_name")+"_"+row.getFloat("performance"));
        }
        results = session.execute("SELECT * FROM topdestinationbyairport WHERE airport_name='LAX' LIMIT 10;");
        for (Row row : results) {
            System.out.println(row.getString("airport_name") + "    " + row.getString("destination_name")+"_"+row.getFloat("performance"));
        }
        results = session.execute("SELECT * FROM topdestinationbyairport WHERE airport_name='IAH' LIMIT 10;");
        for (Row row : results) {
            System.out.println(row.getString("airport_name") + "    " + row.getString("destination_name")+"_"+row.getFloat("performance"));
        }
        results = session.execute("SELECT * FROM topdestinationbyairport WHERE airport_name='SFO' LIMIT 10;");
        for (Row row : results) {
            System.out.println(row.getString("airport_name") + "    " + row.getString("destination_name")+"_"+row.getFloat("performance"));
        }
        
        cluster.close();
    }

}

