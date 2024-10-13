import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockCount {

    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stock = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 分割输入数据
            String[] fields = value.toString().split(",");
            if (fields.length > 3) { // 确保有 stock 列
                // 获取"stock"列的数据并去掉多余的空格
                stock.set(fields[fields.length-1].trim());
                context.write(stock, one);
            }
        }
    }

    public static class StockReducer extends Reducer<Text, IntWritable, Text, Text> {
        // 使用Map保存股票代码及其计数
        private Map<String, Integer> stockCountMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            stockCountMap.put(key.toString(), sum); // 保存每个股票代码及其出现次数
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将 map 转换为 list，便于排序
            List<Map.Entry<String, Integer>> sortedStockList = new ArrayList<>(stockCountMap.entrySet());
            
            // 按照出现次数从大到小排序
            Collections.sort(sortedStockList, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            // 输出排序后的股票代码及其出现次数，按排名格式输出
            int rank = 1;
            for (Map.Entry<String, Integer> entry : sortedStockList) {
                String outputValue = rank + ": " + entry.getKey() + ", " + entry.getValue();
                context.write(new Text(outputValue), null);  // 输出格式为 "<排名>:<股票代码>,<次数>"
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock count");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);  // 由于输出格式变化，将输出值类型改为 Text
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
