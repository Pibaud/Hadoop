import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // Nouvelle API
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RequetesPrincipales {
    // Chemins (Assurez-vous qu'ils existent sur HDFS)
    private static final String INPUT_PATH_CONTENU = "input-requetes/contenu.csv";
    private static final String INPUT_PATH_STREAM = "input-requetes/stream_fact.csv";
    private static final String OUTPUT_PATH = "output/requetesPrincipales-";

    public static class StatsTuple implements Writable {
        private String genre;
        private double revenue;

        // Constructeur vide OBLIGATOIRE pour Hadoop (désérialisation)
        public StatsTuple() {}

        public StatsTuple(String genre, double revenue) {
            this.genre = genre;
            this.revenue = revenue;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeChars(genre);
            out.writeDouble(revenue);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            genre = in.readLine();
            revenue = in.readInt();
        }

    }

    // 1. MAPPER
    // Sortie : Clé = ID Client (Text), Valeur = Tag + Data (Text)
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            if(((FileSplit)context.getInputSplit()).getPath().getName().contains("contenu")){ // mettre un identifiant
                // tring genre = words[]; // mettre l'index
                // ID contenu
                String idContenu = words[0];
                context.write(new Text(idContenu), new Text("GENRE|"+line));
            }else{
                String idContenu = words[0];
                context.write(new Text(idContenu), new Text("FACT|"+line));

            }


        }
    }

    // 2. REDUCER
    public static class JoinReducer extends Reducer<Text, Text, Text, Double> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genreData = null;
            List<String> Facts = new ArrayList<>();

            // On sépare les données reçues pour cet ID Client
            for (Text val : values) {
                String content = val.toString();

                if(content.contains("GENRE")){
                    genreData = content.substring(content.indexOf("|")+1);
                }else{
                    Facts.add(content.substring(content.indexOf("|")+1));
                }
            }

            // Si on a trouvé le client ET qu'il a des commandes

            double revenue = 0.;
            if (genreData != null && !Facts.isEmpty()) {
                for (String fact : Facts) {

                    revenue+=Double.parseDouble(fact.split(",")[9]);

                }
                context.write(new Text(genreData.split(",")[5]),revenue);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "RequetesPrincipales");

        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        // Clé de sortie du Mapper (ID Client)
        job.setOutputKeyClass(Text.class);
        // Valeur de sortie du Mapper (Ligne tagguée)
        job.setOutputValueClass(Double.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // CORRECTION ICI : Ajout des deux chemins correctement
        // FileInputFormat.addInputPath(job, new Path(INPUT_PATH_CUSTOMERS));
        // FileInputFormat.addInputPath(job, new Path(INPUT_PATH_ORDERS));
        FileInputFormat.addInputPaths(job,INPUT_PATH_CONTENU+","+INPUT_PATH_STREAM);

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}