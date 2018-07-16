package iotek.jar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Administrator on 2018/7/7.
 */
//输入的键值对类型  keyin, valuein，输出的键值对类型keyout, valueout
//reducer阶段输入的键值对类型必须是mapper阶段输出的键值对类型
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
    private long count;
    private LongWritable lCount = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> iters =  values.iterator();
        count = 0;
        while (iters.hasNext()){
            LongWritable l = iters.next();
            count += l.get();
        }
        lCount.set(count);
        context.write(key, lCount);
    }
}