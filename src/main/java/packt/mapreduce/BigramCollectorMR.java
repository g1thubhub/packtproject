package packt.mapreduce;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import static packt.UtilitiesJava.createBigrams;

/**
 * Implementation of distinct bigrams in MapReduce
 *
 * @author  Phil, https://github.com/g1thubhub
 */
public class BigramCollectorMR {

    static class BigramCreator extends Mapper<Object, Text, Text, NullWritable> {
        private final Text bigramKey = new Text();
        private NullWritable nil = NullWritable.get();

        @Override
        public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            List<String> bigrams = createBigrams(value.toString());
            for(String bigram : bigrams) {
                bigramKey.set(bigram);
                ctx.write(bigramKey, nil);
            }
        }
    }

    static class BigramPrinter extends Reducer<Text, NullWritable, Text, NullWritable> {
        private final NullWritable nil = NullWritable.get();

        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context ctx) throws IOException, InterruptedException {
            ctx.write(key, nil);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce BigramCollectorMR");
        job.setJarByClass(BigramCollectorMR.class);
        job.setMapperClass(BigramCreator.class);
        job.setReducerClass(BigramPrinter.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(BigramCollectorMR.class.getClass().getResource("/mapreduce/HoD.txt").getPath()));
        FileOutputFormat.setOutputPath(job, new Path("/Users/a/buchin/outbigrams"));   // ToDo: Use relative paths for the following 2
        System.exit(job.waitForCompletion(true) ? 0 : 1); // actual launch
    }

}
