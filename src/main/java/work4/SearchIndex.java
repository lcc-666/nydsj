package work4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SearchIndex {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SearchIndex.class);
        job.setMapperClass(SearchIndexMapper.class);
        job.setReducerClass(SearchIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("input/index"));
        FileOutputFormat.setOutputPath(job, new Path("output/index"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
