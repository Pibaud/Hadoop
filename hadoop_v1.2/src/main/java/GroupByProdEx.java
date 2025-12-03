
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class GroupByProdEx {
    private static final String INPUT_PATH = "input-groupBy/";
    private static final String OUTPUT_PATH = "output/groupBy-";
    private static final Logger LOG = Logger.getLogger(GroupByProdEx.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public static class StatsTuple implements Writable {
        private int count;
        private int quantity;

        // Constructeur vide OBLIGATOIRE pour Hadoop (désérialisation)
        public StatsTuple() {}

        public StatsTuple(int count, int quantity) {
            this.count = count;
            this.quantity = quantity;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(count);
            out.writeInt(quantity);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readInt();
            quantity = in.readInt();
        }

        // Getters
        public int getCount() { return count; }
        public int getQuantity() { return quantity; }

        @Override
        public String toString() {
            return count + "\t" + quantity;
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, StatsTuple> {
        private final static IntWritable one = new IntWritable(1);
        private final static String emptyWords[] = { "" };


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");




            if(!words[0].equals("Row ID")){

                // context.write(new Text(words[1]),new ArrayList<>(List.of(new Integer[]{1, Integer.parseInt(words[words.length - 3])})));
                context.write(new Text(words[1]), new StatsTuple(1,Integer.parseInt(words[words.length - 3])));

            }

            // order Date index : 2
            // state index : 10
            // category : 14
            // id customer : 5

            // exercice 3
            // faire un nouveau fichier, changer doublewritable mettre les 2 valeurs
            // réger le reduce en fonction


        }
    }

    public static class Reduce extends Reducer<Text, StatsTuple, Text, StatsTuple> {
        @Override
        public void reduce(Text key, Iterable<StatsTuple> values, Context context)
                throws IOException, InterruptedException {

            int totalExemplaires=0;
            int totalDistincts=0;
            for (StatsTuple val : values) {
                totalExemplaires+=val.getQuantity();
                totalDistincts+=val.getCount();
            }

            context.write(key, new StatsTuple(totalDistincts,totalExemplaires));


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "GroupBy");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputValueClass(StatsTuple.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}