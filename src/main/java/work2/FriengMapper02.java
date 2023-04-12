package work2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriengMapper02 extends Mapper<LongWritable, Text, Text, Text>{

    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        //1：获取一行
        String line = value.toString();
        //2：切割数据
        String[] fileds = line.split("\t");
        String friend = fileds[0];
        String[] persons = fileds[1].split(",");
        Arrays.sort(persons);//排序
        for (int i = 0; i < persons.length; i++) {
            for (int j = i+1; j < persons.length; j++) {
                context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
            }
        }
    }

}