package work2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FriendDriver01 {

    public static void main(String[] args) throws Exception {
        //1：获取Job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //2:设置jar
        job.setJarByClass(FriendDriver01.class);

        //3:关联Mapper和reducer
        job.setMapperClass(FriendMapper01.class);
        job.setReducerClass(FriendReducer01.class);

        //4:设置mapper输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //5：设置最终输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //6:设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path("input/friends"));
        FileOutputFormat.setOutputPath(job, new Path("output/friends"));

        //7：提交
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }

}