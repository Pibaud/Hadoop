import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class RequeteGenreRevenu {
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

        HashMap<String,String> contenuGenre = new HashMap<>();


        // La méthode setup est appelée UNE FOIS au démarrage du Mapper
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // On charge le fichier de référence depuis HDFS
            FileSystem fs = FileSystem.get(context.getConfiguration());

            // Lecture du fichier contenu.csv
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(INPUT_PATH_CONTENU))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(","); // ou split("\\|")
                    // Supposons: Index 0 = ID, Index 5 = Genre
                    if (parts.length > 5) {
                        contenuGenre.put(parts[0], parts[5]);
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            if(!line.contains("ID_CONTENU")) {
                if (((FileSplit) context.getInputSplit()).getPath().getName().contains("contenu")) { // mettre un identifiant
                    // tring genre = words[]; // mettre l'index
                    // ID contenu
                    String idContenu = words[0];
                    String genre = words[5];
                    contenuGenre.put(idContenu, genre);
                    context.write(new Text(genre), new Text("CONTENU|" + line));
                } else {
                    String idContenu = words[5];
                    String genre = contenuGenre.get(idContenu);
                    context.write(new Text(genre), new Text("FACT|" + line));

                }
            }
        }
    }

    // 2. REDUCER
    public static class JoinReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {



            String contenuData = null;
            List<String> Facts = new ArrayList<>();

            // On sépare les données reçues pour cet ID Client
            for (Text val : values) {
                String content = val.toString();

                if(content.contains("CONTENU")){
                    contenuData = content.substring(8);
                }else{
                    Facts.add(content.substring(5));
                }
            }

            // Si on a trouvé le client ET qu'il a des commandes

            double revenue = 0.;
            if (contenuData != null && !Facts.isEmpty()) {
                System.out.println("ALOOOOOOOOOOo" +contenuData);
                for (String fact : Facts) {

                    String[] attributs =  fact.split(",");

                    if(attributs[0].length()>=7) {
                        revenue += Double.parseDouble(attributs[7]);
                    }

                }
                String[] genre =  contenuData.split(",");
                if(genre.length>=5) {
                    context.write(new Text(genre[5]), new DoubleWritable(revenue));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "RequetesPrincipales");

        // CORRECTION 1 : La bonne classe principale
        job.setJarByClass(RequeteGenreRevenu.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        // CORRECTION 2 : DÉFINIR EXPLICITEMENT LA SORTIE DU MAPPER
        // Le mapper sort <Text, Text>
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // CORRECTION 3 : DÉFINIR LA SORTIE FINALE (REDUCER)
        // Le reducer sort <Text, DoubleWritable>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); // Utiliser Writable !

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPaths(job, INPUT_PATH_CONTENU + "," + INPUT_PATH_STREAM);
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}