package repeat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DIsRepeat2 {
    public static class DisRepeatMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
        private Text tWord=new Text();
        private NullWritable nul=NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            String[] words = value.toString().split(" ");
            for (String word : words) {
                tWord.set(word);
                context.write(tWord, nul);
            }
        }
    }
    public static class DisRepeatReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        private NullWritable nul=NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            context.write(key, nul);
        }
    }
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
        System.setProperty("hadoop.home.dir","D:\\bigdatalesson\\hadoop2.6.5-bin");
        Configuration conf=new Configuration();
        Job job= Job.getInstance(conf);

        //指出打包整个job的类

    }
}
