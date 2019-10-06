package packt.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static packt.UtilitiesJava.tokenizeSimple;

/**
 * Implementation of WordCount in MapReduce
 *
 * @author  Phil, https://github.com/g1thubhub
 */
public class WordCountMR {

    static class WordTokenizer extends Mapper<Object, Text, Text, LongWritable> {
        private Text token = new Text();
        private final static LongWritable one = new LongWritable(1L);

        @Override
        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            StringTokenizer tokenizer = tokenizeSimple(value.toString()); // local Java function
            while (tokenizer.hasMoreTokens()) { // iterate through tokens and emit each one with a count of 1
                token.set(tokenizer.nextToken());
                ctx.write(token, one);
            }
        }

    }

    static class WordCounter extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
            long count = 0L; // overall sum for each word
            for (LongWritable value : values) {
                count += value.get();
            }
            ctx.write(key, new LongWritable(count)); // emit <word, total count> pair
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); // setup stuff
        Job job = Job.getInstance(conf, "MapReduce WordCountMR");
        job.setJarByClass(WordCountMR.class);
        job.setMapperClass(WordTokenizer.class);
//        job.setCombinerClass(WordCounter.class); // optional Combiner optimization
        job.setReducerClass(WordCounter.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(WordCountMR.class.getClass().getResource("/mapreduce/HoD.txt").getPath()));
        FileOutputFormat.setOutputPath(job, new Path("/Users/a/buchin/out"));   // ToDo: Use relative paths for the following 2
        System.exit(job.waitForCompletion(true) ? 0 : 1); // actual launch
    }

}
