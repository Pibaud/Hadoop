import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // Nouvelle API
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class RequetePaysAgeRevenu {
    // Chemins (Assurez-vous qu'ils existent sur HDFS)
    private static final String INPUT_PATH_CONTENU = "input-requetes/contenu.csv";
    private static final String INPUT_PATH_STREAM = "input-requetes/stream_fact.csv";
    private static final String INPUT_PATH_USER = "input-requetes/utilisateurs.csv";
    private static final String OUTPUT_PATH = "output/requetesPrincipales-";

    // IL FAUT WritableComparable, pas juste Writable
    public static class StatsTuple implements WritableComparable<StatsTuple> {
        private String pays;
        private String age;

        public StatsTuple() {}

        public StatsTuple(String pays, String age) {
            this.pays = pays;
            this.age = age;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // writeUTF est plus sûr que writeChars pour des Strings courtes
            // Il écrit la longueur puis les caractères
            out.writeUTF(pays);
            out.writeUTF(age);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // Il faut lire EXACTEMENT comme on a écrit
            pays = in.readUTF();
            age = in.readUTF();
        }

        // C'EST CETTE MÉTHODE QUI MANQUAIT ET QUI CAUSAIT LE PLANTAGE
        @Override
        public int compareTo(StatsTuple o) {
            // 1. On compare les pays alphabétiquement
            int cmp = this.pays.compareTo(o.pays);

            // 2. Si c'est le même pays, on compare les âges
            if (cmp == 0) {
                return this.age.compareTo(o.age);
            }

            return cmp;
        }

        // Important pour l'écriture finale dans le fichier texte
        @Override
        public String toString() {
            return pays + "\t" + age;
        }

        // HashCode est recommandé pour que le partitionnement soit efficace
        @Override
        public int hashCode() {
            return pays.hashCode() * 163 + age.hashCode();
        }
    }

    // 1. MAPPER
    // Sortie : Clé = ID Client (Text), Valeur = Tag + Data (Text)
    public static class JoinMapper extends Mapper<LongWritable, Text, StatsTuple, DoubleWritable> {

        HashMap<String,StatsTuple> userPayAge = new HashMap<>();


        // La méthode setup est appelée UNE FOIS au démarrage du Mapper
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // On charge le fichier de référence depuis HDFS
            FileSystem fs = FileSystem.get(context.getConfiguration());

            // Lecture du fichier contenu.csv
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(INPUT_PATH_USER))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(","); // ou split("\\|")x²
                    // Supposons: Index 0 = ID, Index 5 = Genre
                    if (parts.length > 5) {
                        userPayAge.put(parts[0],new StatsTuple(parts[2],parts[5]));
                    }
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            if(!line.contains("ID_CONTENU")) {
                if (((FileSplit) context.getInputSplit()).getPath().getName().contains("fact")) { // mettre un identifiant
                    // tring genre = words[]; // mettre l'index
                    // ID contenu
                    String idUser = words[2];
                    double revenu = Double.parseDouble(words[10]);

                    StatsTuple payAge = userPayAge.get(idUser);

                    context.write(payAge,new DoubleWritable(revenu));
                }
            }
        }
    }

    // 2. REDUCER
    public static class JoinReducer extends Reducer<StatsTuple, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(StatsTuple key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {


            double revenu = 0;
            int nbValues = 0;
            for(DoubleWritable value : values) {
                revenu += value.get();
                nbValues++;
            }

            context.write(new Text(key.toString()),new DoubleWritable(revenu/nbValues));


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "RequetesPrincipales");

        // CORRECTION 1 : La bonne classe principale
        job.setJarByClass(RequetePaysAgeRevenu.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        // CORRECTION 2 : DÉFINIR EXPLICITEMENT LA SORTIE DU MAPPER
        // Le mapper sort <Text, Text>
        job.setMapOutputKeyClass(StatsTuple.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // CORRECTION 3 : DÉFINIR LA SORTIE FINALE (REDUCER)
        // Le reducer sort <Text, DoubleWritable>
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class); // Utiliser Writable !

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path( INPUT_PATH_STREAM));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}