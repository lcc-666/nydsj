package work2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendMapper01 extends Mapper<LongWritable, Text, Text, Text>{

    Text k  =new Text();
    Text v  =new Text();

    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        //1：获取一行数据
        String line = value.toString();
        //2：对一行数据进行切割
        String[] fields = line.split(":");
        String person = fields[0];
        String[] friends = fields[1].split(",");
        for (String friend : friends) {
            k.set(friend);
            v.set(person);
            context.write(k, v);
        }
    }
}
