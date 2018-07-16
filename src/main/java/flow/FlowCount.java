package flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Administrator on 2018/7/14.
 */
public class FlowCount {
    public static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
        private Text tPhone = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FlowBean flowBean = new FlowBean();
            String[] words = value.toString().split("\t");
            tPhone.set(words[1]);

            flowBean.setPhone(words[1]);

            flowBean.setUpFlow(Long.parseLong(words[7]));
            flowBean.setDownFlow(Long.parseLong(words[8]));
            flowBean.setCountFlow(flowBean.getUpFlow() + flowBean.getDownFlow());

            context.write(tPhone, flowBean);

        }
    }


    public static  class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
        private long upFlow_phone_all,downFlow_phone_all;
        @Override
        protected void reduce(Text phone, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
            upFlow_phone_all = 0;
            downFlow_phone_all = 0;

            FlowBean flowBean_all = new FlowBean();
            flowBean_all.setPhone(phone.toString());

            for (FlowBean fb : values) {
                upFlow_phone_all += fb.getUpFlow();
                downFlow_phone_all += fb.getDownFlow();
            }
            flowBean_all.setUpFlow(upFlow_phone_all);
            flowBean_all.setDownFlow(downFlow_phone_all);
            flowBean_all.setCountFlow(upFlow_phone_all + downFlow_phone_all);

            context.write(phone, flowBean_all);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\bigdatalesson\\hadoop2.6.5-bin");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //指出打包整个job的类
        job.setJarByClass(FlowCount.class);

        //指明执行map任务和reduce任务的类
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        //指出map阶段的输出键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //指出reduce阶段的输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //这里的路径指的是hdfs的路径
        FileInputFormat.setInputPaths(job, new Path("d:\\input"));
        //输出路径必须是不存在的，否则会报错
        FileOutputFormat.setOutputPath(job, new Path("d:\\output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
