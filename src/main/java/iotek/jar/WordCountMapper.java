package iotek.jar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2018/7/7.
 */
//输入的键值对类型  keyin, valuein，输出的键值对类型keyout, valueout
public class WordCountMapper extends Mapper<LongWritable, Text,  Text, LongWritable> {
    private Text tWord = new Text();
    private LongWritable one = new LongWritable(1);
    @Override
    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        String str = line.toString();
        String[] words = str.split(" ");//切割一行文字为一个字符串数组

        for(String word:words){      //遍历素组，输出<单词， 1>
            tWord.set(word);
            context.write(tWord, one);
        }
    }
}
