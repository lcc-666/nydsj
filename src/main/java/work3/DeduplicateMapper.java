package work3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DeduplicateMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    // <0,2022-12-3 c> --> <2022-12-3 c,null>
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}
