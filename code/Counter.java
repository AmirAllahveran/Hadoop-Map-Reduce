import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Counter {

    public static class T_Mapper extends Mapper<Object, Text, Text, Text>{
        Text textKey = new Text();
        Text textValue = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString().replace("\\",".");
            if(!valueString.contains("created_at,tweet_id,tweet,likes,retweet_count,source,user_id,user_name")){
                CSVReader R = new CSVReader(new StringReader(valueString));
                String[] parsedLine = R.readNext();
                R.close();
                String tweet = parsedLine[2];
                String likeNum = parsedLine[3];
                String retweetNum = parsedLine[4];
                textValue.set(likeNum +" "+ retweetNum);
                if((tweet.contains("#Biden") || tweet.contains("#JoeBiden")) && (tweet.contains("#Trump") || tweet.contains("#DonaldTrump"))){
                    textKey.set("Both");
                    context.write(textKey,textValue);
                }
                else if (tweet.contains("#Biden") || tweet.contains("#JoeBiden")){
                    textKey.set("Joe Biden");
                    context.write(textKey,textValue);
                }
                else if (tweet.contains("#Trump") || tweet.contains("#DonaldTrump")){
                    textKey.set("Donald Trump");
                    context.write(textKey,textValue);
                }
            }
        }
    }

    public static class T_Reducer extends Reducer<Text,Text,Text,Text> {
        Text textValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float likeSum = 0;
            float retweetSum = 0;
            for (Text val : values) {
                String line = val.toString();
                String[] field = line.split(" ");
                likeSum += Float.parseFloat(field[0]);
                retweetSum += Float.parseFloat(field[1]);
            }
            textValue.set(likeSum + " " + retweetSum);
            context.write(key, textValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "like and retweet count");
        job.setJarByClass(Counter.class);
        job.setMapperClass(T_Mapper.class);
        job.setReducerClass(T_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
