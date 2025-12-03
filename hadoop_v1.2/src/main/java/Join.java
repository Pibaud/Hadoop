import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // Nouvelle API
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join {
    // Chemins (Assurez-vous qu'ils existent sur HDFS)
    private static final String INPUT_PATH_CUSTOMERS = "input-join/customers.tbl";
    private static final String INPUT_PATH_ORDERS = "input-join/orders.tbl";
    private static final String OUTPUT_PATH = "output/join-";

    // 1. MAPPER
    // Sortie : Clé = ID Client (Text), Valeur = Tag + Data (Text)
    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("\\|");

            // Logique de détection (à adapter selon votre format de fichier exact TPC-H)
            // Supposons : Customers (Clé index 0), Orders (Clé Client index 1)

            // Astuce : On "tag" la donnée pour que le Reducer sache d'où elle vient
            if (line.contains("Customer")) { // Condition simplifiée pour l'exemple
                // C'est un CLIENT
                // Exemple: words[0] est l'ID Client
                String idClient = words[0];
                context.write(new Text(idClient), new Text("CUST|" + line));
            } else {
                // C'est une COMMANDE
                // Exemple: words[1] est l'ID Client dans la table commande
                String idClientRef = words[1];
                context.write(new Text(idClientRef), new Text("ORDER|" + line));
            }
        }
    }

    // 2. REDUCER
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String customerData = null;
            List<String> ordersList = new ArrayList<>();

            // On sépare les données reçues pour cet ID Client
            for (Text val : values) {
                String content = val.toString();

                if (content.startsWith("CUST|")) {
                    // On retire le tag
                    customerData = content.substring(5);
                } else if (content.startsWith("ORDER|")) {
                    ordersList.add(content.substring(6));
                }
            }

            // Si on a trouvé le client ET qu'il a des commandes
            if (customerData != null && !ordersList.isEmpty()) {
                for (String order : ordersList) {

                    context.write(key,new Text(customerData.split("\\|")[7] +" | "+ order.split("\\|")[8]));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Join");

        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        // Clé de sortie du Mapper (ID Client)
        job.setOutputKeyClass(Text.class);
        // Valeur de sortie du Mapper (Ligne tagguée)
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // CORRECTION ICI : Ajout des deux chemins correctement
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH_CUSTOMERS));
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH_ORDERS));

        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}