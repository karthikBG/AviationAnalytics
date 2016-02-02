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

public class TopAirlines {
    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

    public static class AirlineCountMap extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] contents = value.toString().split(",");

            if (contents.length > 0 && !contents[0].equalsIgnoreCase("year") && !contents[18].equalsIgnoreCase("1") && !contents[13].isEmpty()) {
                context.write(new Text(contents[8]), new IntWritable((int)Float.parseFloat(contents[13])));
            }
        }
    }

    public static class AirlineCountReduce extends Reducer<Text, IntWritable, Text, FloatWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }

            if (count > 0) {
                float avg = (float) sum/ (float)count;
                context.write(key, new FloatWritable(avg));
            }
        }
    }

    public static class TopAirlinesMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        private TreeSet<Pair<Float, String>> countToAirlineMap = new TreeSet<Pair<Float, String>>();

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Float count = Float.parseFloat(value.toString());
            String Airline = key.toString();

            countToAirlineMap.add(new Pair<Float, String>(count, Airline));

            if (countToAirlineMap.size() > 10) {
                countToAirlineMap.remove(countToAirlineMap.first());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Pair<Float, String> item : countToAirlineMap) {
                String[] strings = {item.second, item.first.toString()};
                TextArrayWritable val = new TextArrayWritable(strings);
                context.write(NullWritable.get(), val);
            }
        }
    }

    public static class TopAirlinesReduce extends Reducer<NullWritable, TextArrayWritable, Text, FloatWritable> {
        private TreeSet<Pair<Float, String>> countToAirlineMap = new TreeSet<Pair<Float, String>>();

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
            for (TextArrayWritable val : values) {
                Text[] pair = (Text[]) val.toArray();

                String Airline = pair[0].toString();
                Float count = Float.parseFloat(pair[1].toString());

                countToAirlineMap.add(new Pair<Float, String>(count, Airline));

                if (countToAirlineMap.size() > 10) {
                    countToAirlineMap.remove(countToAirlineMap.first());
                }
            }

            for (Pair<Float, String> item : countToAirlineMap) {
                Text Airline = new Text(item.second);
                FloatWritable value = new FloatWritable(item.first);
                context.write(Airline, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/user/ec2-user/tmp");
        fs.delete(tmpPath, true);


        Job jobA = Job.getInstance(conf, "AirlineDelayCount");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirlineCountMap.class);
        jobA.setReducerClass(AirlineCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopAirlines.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airlines");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(FloatWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirlinesMap.class);
        jobB.setReducerClass(TopAirlinesReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopAirlines.class);
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
