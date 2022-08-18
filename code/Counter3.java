import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Counter3 {

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
                String s_c_lat = parsedLine[13];
                String s_c_long = parsedLine[14];
                if (!s_c_lat.equals("") && !s_c_long.equals("")){
                    float c_lat = Float.parseFloat(s_c_lat);
                    float c_long = Float.parseFloat(s_c_long);
                    String selectedCountry = "";
                    if (c_lat > 19.5 && c_lat < 64.85 && c_long > -161.75 && c_long < -68){
                        selectedCountry = "America";
                    }else if (c_lat > 41.6 && c_lat < 51 && c_long > -4.65 && c_long < 9.45){
                        selectedCountry = "France";
                    }
                    if (!selectedCountry.equals("")){
                        textKey.set(selectedCountry);
                        if ((tweet.contains("#Biden") || tweet.contains("#JoeBiden")) && (tweet.contains("#Trump") || tweet.contains("#DonaldTrump"))){
                            textValue.set("1.0 0.0 0.0 1.0");
                            context.write(textKey,textValue);
                        }else if (tweet.contains("#Trump") || tweet.contains("#DonaldTrump")){
                            textValue.set("0.0 0.0 1.0 1.0");
                            context.write(textKey,textValue);
                        }else if (tweet.contains("#Biden") || tweet.contains("#JoeBiden")){
                            textValue.set("0.0 1.0 0.0 1.0");
                            context.write(textKey,textValue);
                        }
                    }
                }


            }
        }
    }

    public static class T_Reducer extends Reducer<Text,Text,Text,Text> {
        Text textValue = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float bothSum = 0;
            float bidenSum = 0;
            float trumpSum = 0;
            float allCount = 0;
            for (Text val : values) {
                String line = val.toString();
                String[] field = line.split(" ");
                bothSum += Float.parseFloat(field[0]);
                bidenSum +=  Float.parseFloat(field[1]);
                trumpSum +=  Float.parseFloat(field[2]);
                allCount +=  Float.parseFloat(field[3]);
            }
            float bothPercent = bothSum/allCount;
            float bidenPercent = bidenSum/allCount;
            float trumpPercent = trumpSum/allCount;

            textValue.set(bothPercent + " " + bidenPercent +" "+ trumpPercent + " "+allCount);
            context.write(key, textValue);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "countries tweet count v2");
        job.setJarByClass(Counter3.class);
        job.setMapperClass(T_Mapper.class);
        job.setReducerClass(T_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
