package work5;

import java.io.IOException;
import java.util.PriorityQueue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {

    private final PriorityQueue<Integer> maxHeap = new PriorityQueue<>((a, b) -> b - a);

    @Override
    protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context) {
        for (IntWritable value : values) {
            int num = value.get();
            // 求前k大的数
            int k = 10;
            if (maxHeap.size() < k) {
                maxHeap.offer(num);
            } else if (maxHeap.peek() < num) {
                maxHeap.poll();
                maxHeap.offer(num);
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!maxHeap.isEmpty()) {
            context.write(NullWritable.get(), new IntWritable(maxHeap.poll()));
        }
    }
}
