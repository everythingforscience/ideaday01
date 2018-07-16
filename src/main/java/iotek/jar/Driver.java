package iotek.jar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/7/7.
 */
public class Driver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指出打包整个job的类
        job.setJarByClass(Driver.class);

        //指明执行map任务和reduce任务的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指出map阶段的输出键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指出reduce阶段的输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //这里的路径指的是hdfs的路径
        FileInputFormat.setInputPaths(job, new Path("/input"));
        //输出路径必须是不存在的，否则会报错
        FileOutputFormat.setOutputPath(job, new Path("/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
