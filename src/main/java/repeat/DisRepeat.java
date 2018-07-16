package repeat;

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

/**
 * Created by Administrator on 2018/7/7.
 */
public class DisRepeat {

    public static class DisRepeatMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
        private Text tWord = new Text();
        private NullWritable nul = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word:words){
                tWord.set(word);
                context.write(tWord, nul);
            }
        }
    }

    public static class DisRepeatReducer extends Reducer<Text, NullWritable, Text, NullWritable>{
        private NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, nul);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        System.setProperty("hadoop.home.dir", "D:\\bigdatalesson\\hadoop2.6.5-bin");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指出打包整个job的类
        job.setJarByClass(DisRepeat.class);

        //指明执行map任务和reduce任务的类
        job.setMapperClass(DisRepeatMapper.class);
        job.setReducerClass(DisRepeatReducer.class);

        //指出map阶段的输出键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指出reduce阶段的输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //这里的路径指的是hdfs的路径
        FileInputFormat.setInputPaths(job, new Path("c:\\input"));
        //输出路径必须是不存在的，否则会报错
        FileOutputFormat.setOutputPath(job, new Path("c:\\output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
