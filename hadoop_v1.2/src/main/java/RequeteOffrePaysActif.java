import java.io.*;
import java.time.Instant;
import java.util.HashMap;

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

public class RequeteOffrePaysActif {
    // Chemins (Assurez-vous qu'ils existent sur HDFS)
    private static final String INPUT_PATH_DATE = "input-requetes-secondaires/date_dim.csv";
    private static final String INPUT_PATH_ABONNEMENTS = "input-requetes-secondaires/abonnement_fact.csv";
    private static final String INPUT_PATH_REGION= "input-requetes-secondaires/region_dim.csv";
    private static final String INPUT_PATH_OFFRE="input-requetes-secondaires/offre_dim.csv";
    private static final String OUTPUT_PATH = "output/requetesSecondaireOffrePaysActif-";

    // IL FAUT WritableComparable, pas juste Writable
    public static class StatsTuple implements WritableComparable<StatsTuple> {
        private String pays;
        private String offre;
        public StatsTuple() {}

        public StatsTuple(String pays, String offre) {
            this.pays = pays;
            this.offre=offre;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            // writeUTF est plus sûr que writeChars pour des Strings courtes
            // Il écrit la longueur puis les caractères
            out.writeUTF(pays);
            out.writeUTF(offre);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // Il faut lire EXACTEMENT comme on a écrit
            pays = in.readUTF();
            offre = in.readUTF();

        }

        // C'EST CETTE MÉTHODE QUI MANQUAIT ET QUI CAUSAIT LE PLANTAGE
        @Override
        public int compareTo(StatsTuple o) {
            // 1. On compare les pays alphabétiquement
            int cmp = this.pays.compareTo(o.pays);

            // 2. Si c'est le même pays, on compare les âges
            if (cmp == 0) {

                return this.offre.compareTo(o.offre);

            }

            return cmp;
        }

        // Important pour l'écriture finale dans le fichier texte
        @Override
        public String toString() {
            return pays + "\t" + offre;
        }

        // HashCode est recommandé pour que le partitionnement soit efficace
        @Override
        public int hashCode() {
            return pays.hashCode() * 163 + offre.hashCode();
        }
    }

    // 1. MAPPER
    // Sortie : Clé = ID Client (Text), Valeur = Tag + Data (Text)
    public static class JoinMapper extends Mapper<LongWritable, Text, StatsTuple, DoubleWritable> {

        HashMap<String,StatsTuple> nomOffre = new HashMap<>();
        HashMap<String,StatsTuple> regionNom = new HashMap<>();

        // La méthode setup est appelée UNE FOIS au démarrage du Mapper
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // On charge le fichier de référence depuis HDFS
            FileSystem fs = FileSystem.get(context.getConfiguration());

            // Lecture du fichier contenu.csv
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(INPUT_PATH_OFFRE))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(","); // ou split("\\|")x²
                    // Supposons: Index 0 = ID, Index 5 = Genre
                    if (parts.length > 2) {
                        nomOffre.put(parts[0],new StatsTuple("",parts[1]));
                    }
                }
            }

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(INPUT_PATH_REGION))))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(","); // ou split("\\|")x²
                    // Supposons: Index 0 = ID, Index 5 = Genre
                    if (parts.length > 3) {
                        regionNom.put(parts[0],new StatsTuple(parts[2],""));
                    }
                }
            }


        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            if(!line.contains("ID_")) {
                if (((FileSplit) context.getInputSplit()).getPath().getName().contains("fact")) { // mettre un identifiant
                    // tring genre = words[]; // mettre l'index
                    // ID contenu
                    String idOffre = words[1];
                    StatsTuple annee =  nomOffre.get(idOffre);

                    String idRegion = words[2];
                    StatsTuple region = regionNom.get(idRegion);

                    double nbAbonnements = Double.parseDouble(words[4]);

                    context.write(new StatsTuple(region.pays, annee.offre),new DoubleWritable(nbAbonnements));


                }
            }
        }
    }

    // 2. REDUCER
    public static class JoinReducer extends Reducer<StatsTuple, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(StatsTuple key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {


            double nbAbonnementActif = 0;
            for(DoubleWritable value : values) {
                nbAbonnementActif += value.get();

            }

            context.write(new Text(key.toString()),new DoubleWritable(nbAbonnementActif));


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "RequetesPrincipales");

        // CORRECTION 1 : La bonne classe principale
        job.setJarByClass(RequeteOffrePaysActif.class);

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

        FileInputFormat.addInputPath(job, new Path( INPUT_PATH_ABONNEMENTS));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}