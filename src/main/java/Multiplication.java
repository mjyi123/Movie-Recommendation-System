import com.sun.org.apache.xpath.internal.operations.Mult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {

    public static class CooccurrenceMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] line = value.toString().trim().split("\t");
            context.write(new Text(line[0]), new Text(line[1]));

        }
    }

    public static class RatingMapper extends
            Mapper<Text, Text, Text, Text> {
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] line = value.toString().trim().split(",");
            context.write(new Text(line[1]), new Text(line[0] + ":" + line[2]));

        }
    }

    public static class MultiplicationReducer extends
            Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {

            Map<String, Double> relationMap = new HashMap<String, Double>();
            Map<String, Double> ratingMap = new HashMap<String, Double>();

            for(Text value : values) {
                if(value.toString().contains("=")) {
                    String[] movie_relation = value.toString().trim().split("=");
                    relationMap.put(movie_relation[0], Double.parseDouble(movie_relation[1]));
                } else {
                    String[] user_rating = value.toString().trim().split(":");
                    ratingMap.put(user_rating[0], Double.parseDouble(user_rating[1]));
                }
            }

            for(Map.Entry<String, Double> entry : relationMap.entrySet()) {

                String movieA = entry.getKey();
                Double relation = entry.getValue();

                for(Map.Entry<String, Double> element : ratingMap.entrySet() ) {
                    String user = element.getKey();
                    Double rating = element.getValue();

                    String outputkey = user + ":" + movieA;
                    double outputValue = relation * rating;
                    context.write(new Text(outputkey), new DoubleWritable(outputValue));
                }

            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(CooccurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(MultiplicationReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), org.apache.hadoop.mapred.TextInputFormat.class, CooccurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), org.apache.hadoop.mapred.TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
