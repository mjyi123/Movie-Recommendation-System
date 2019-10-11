import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

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

public class Normalize {

    public static class NormalizeMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] movie_relation = value.toString().trim().split("\t");
            if(movie_relation.length != 2) {
                return;;
            }

            String movieA = movie_relation[0].split(":")[0];
            String movieB = movie_relation[0].split(":")[1];
            context.write(new Text(movieA), new Text(movieB + "=" + movie_relation[1]));


        }
    }

    public static class NormalizeReducer extends
            Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            Map<String, Integer> map =new HashMap<String, Integer>();

            for(Text value : values) {
                String[] movie_relation = value.toString().trim().split("=");
                int relation = Integer.parseInt(movie_relation[1]);
                sum += relation;
                map.put(movie_relation[0], relation);
            }

            for(Map.Entry<String, Integer> entry : map.entrySet()) {
                String outputKey = entry.getKey();
                String outputValue = key.toString() + "=" + ((double)entry.getValue()/sum);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
