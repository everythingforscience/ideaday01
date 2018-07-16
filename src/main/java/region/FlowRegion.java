package region;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowRegion {
    public static class FlowRegionMapper extends Mapper<LongWritable, Text, Text, Text>{
        private Text tPhone = new Text();
        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().split("\t");
            tPhone.set(words[1]);

            context.write(tPhone, line);
        }
    }

    public static class FlowRegionReducer extends Reducer<Text, Text, NullWritable, Text>{
        private NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(nul, value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigdatalesson\\hadoop2.6.5-bin");

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(FlowRegion.class);

        //利用自定义的分区类替代系统默认的分区类
        job.setPartitionerClass(RegionHashPartioner.class);

        //启动6个reduce
        job.setNumReduceTasks(6);

        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(FlowRegionMapper.class);
        job.setReducerClass(FlowRegionReducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("D:\\output3"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
