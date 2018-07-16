package region;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.util.HashMap;
import java.util.Map;

//此处的键值对类型对应着mapper类的输出键值对类型
public class RegionHashPartioner extends HashPartitioner<Text, Text> {
    private static Map<String, Integer> map = new HashMap<String, Integer>();

    static {
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }
    @Override
    public int getPartition(Text phone, Text line, int numReduceTasks) {
        String pre3 = phone.toString().substring(0, 3);
        Integer value = map.get(pre3);

        if(value != null){
            return  value;
        }

        return 5;
    }
}
