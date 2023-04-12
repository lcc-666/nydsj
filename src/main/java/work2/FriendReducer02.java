package work2;


import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer02 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        HashSet<String> set = new HashSet<>();

        for (Text value : values) {
            String v = value.toString();
            if(!set.contains(v)) {
                set.add(v);
                sb.append(v).append(",");
            }
        }
        sb.deleteCharAt(sb.length()-1);
        context.write(key, new Text(sb.toString()));
    }
}
