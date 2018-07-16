package flow;

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
 * Created by Administrator on 2018/7/6.
 */
public class FlowSort {

    public static class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
        private NullWritable nul = NullWritable.get();
        @Override
        protected void map(LongWritable key, Text sentence, Context context) throws IOException, InterruptedException {

            FlowBean flowBean = new FlowBean();

            String contents;
            contents = sentence.toString();
            String[] words;
            words = contents.split(",");

            flowBean.setPhone(words[0]);//手机号
            flowBean.setUpFlow(Long.parseLong(words[1]));//上行流量
            flowBean.setDownFlow(Long.parseLong(words[2]));//下行流量
            flowBean.setCountFlow(Long.parseLong(words[3]));
            //flowBean.setCountFlow(flowBean.getUpFlow() + flowBean.getDownFlow());



            context.write(flowBean, nul);
        }
    }

    public static class FlowSortReducer extends Reducer<FlowBean, NullWritable, NullWritable, FlowBean>{

        private NullWritable nul = NullWritable.get();
        @Override
        protected void reduce(FlowBean bean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            context.write(nul, bean);

        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigdatalesson\\hadoop2.6.5-bin");

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        //指出打包的类
        job.setJarByClass(FlowSort.class);
        //mapper任务的类 和Reducer任务的类
        job.setMapperClass(FlowSortMapper.class);
        job.setReducerClass(FlowSortReducer.class);

        //指出map阶段分别对应的输出键值对的类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //reduce阶段输出键值对的类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlowBean.class);

        //指出map阶段要处理数据的路径
        FileInputFormat.setInputPaths(job, new Path("D:\\input2"));
        //指出reduce阶段处理结果的输出路径，必须是不存在的
        FileOutputFormat.setOutputPath(job, new Path("D:\\output2"));

        //提交mr任务到集群，提交完成后返回
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
