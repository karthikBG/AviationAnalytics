import java.io.IOException;
import java.lang.Integer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TopDestinationsByAirport {

    public static class SourceDestinationMap extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] contents = value.toString().split(",");

            if (contents.length > 0 && !contents[0].equalsIgnoreCase("year") && !contents[18].equalsIgnoreCase("1")) {
                String origin = contents[15];
                int delay =(int)(Float.parseFloat(contents[14]));
                String destinationDelay = contents[16] + "_" + delay;
                context.write(new Text(origin), new Text(destinationDelay));
            }
        }
    }

    public static class SourceDestinationReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            StringBuilder airportList = new StringBuilder();   //final value
            Hashtable<String, Integer> AirportDelay = new Hashtable<String, Integer>();
            
           for(Text txt : values){       
                String value =  (txt.toString()).trim();
                String[] st = value.split("_");
                
                String dest = st[0];
                int delay = Integer.parseInt(st[1]);

                if(AirportDelay.containsKey(dest)) {
                    int existDelay = AirportDelay.get(dest);
                    AirportDelay.put(dest, existDelay + delay);
                } else {    
                    AirportDelay.put(dest, delay);
                }
            }

            
            for( String keys: AirportDelay.keySet()) {
                    String display = keys + "_" + AirportDelay.get(keys) + " ";
                    airportList.append(display);
            }
            context.write(new Text(key), new Text(airportList.toString().trim()));
        }
    }

    public static class TopSourceDestinationMap extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            TreeSet<Pair<Integer, String>> countToWordMap = new TreeSet<Pair<Integer, String>>();

            String allValues = value.toString();
            String[] indValues = allValues.split(" ");

            for (String v : indValues) {
                String[] split = v.split("_");
                countToWordMap.add(new Pair<Integer, String>(Integer.parseInt(split[1]), split[0]));
            }

            for (Pair<Integer, String> item : countToWordMap) {
                context.write(new Text(key), new Text(item.second + "_" + item.first));
            }
        }
    }

    public static class TopSourceDestinationReduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            TreeSet<Pair<Integer, String>> countToWordMap = new TreeSet<Pair<Integer, String>>();
            for (Text va : values) {
                String[] val = va.toString().split("_");

                String word = val[0];
                Integer count = Integer.parseInt(val[1]);

                countToWordMap.add(new Pair<Integer, String>(count, word));

                if (countToWordMap.size() > 10) {
                    countToWordMap.remove(countToWordMap.first());
                }
            }

            for (Pair<Integer, String> item : countToWordMap) {
                context.write(new Text(key), new Text(item.second +"_" + item.first));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/user/ec2-user/tmp");
        fs.delete(tmpPath, true);


        Job jobA = Job.getInstance(conf, "DestinationsByAirport");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);

        jobA.setMapperClass(SourceDestinationMap.class);
        jobA.setReducerClass(SourceDestinationReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopSourceDestination.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "TopDestinationByAirport");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);

        jobB.setMapperClass(TopSourceDestinationMap.class);
        jobB.setReducerClass(TopSourceDestinationReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopSourceDestination.class);
        System.exit(jobB.waitForCompletion(true) ? 0 : 1);
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (o.first).compareTo(this.first);
        return cmp == 0 ? (o.second).compareTo(this.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
