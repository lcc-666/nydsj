package work5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] nums = line.split("\\s+");
        for (String num : nums) {
            context.write(NullWritable.get(), new IntWritable(Integer.parseInt(num)));
        }
    }
}
