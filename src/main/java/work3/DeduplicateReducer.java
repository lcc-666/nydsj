package work3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DeduplicateReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    // <2022-12-3 c,null> <2022-12-4 d,null><2022-12-4 d,null>
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
