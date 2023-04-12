package work2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer01 extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        //1：获取哪些好友都有对应的人
        for (Text text : values) {
            sb.append(text.toString()).append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        context.write(key, new Text(sb.toString()));
    }
}