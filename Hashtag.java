import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hashtag {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Create a new job
        Job job = Job.getInstance(conf, "Hashtag");

        // Use the WordCount.class file to point to the job jar
        job.setJarByClass(Hashtag.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Setting the input and output locations
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for it's completion
        job.waitForCompletion(true);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] fields = line.split(" ");

            for (String hashtag : fields) {
                if(hashtag.matches(".*happy*.") || hashtag.matches(".*HAPPY*.")){
                    word.set(hashtag);
                    context.write(word,key);
                }
            }


        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            
            // Sum all the occurrences of the word (key)
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new Text(sum));
        }
    }
}
