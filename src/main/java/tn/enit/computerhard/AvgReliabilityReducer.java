package tn.enit.computerhard;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.io.DoubleWritable;
    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.log4j.LogManager;
    import org.apache.log4j.Logger;

    import java.io.IOException;

    public class AvgReliabilityReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private static final Logger logger = LogManager.getLogger(AvgReliabilityReducer.class);
    
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalReliability = 0;
            int totalCount = 0;
    
            for (DoubleWritable val : values) {
                totalReliability += val.get();
                totalCount++;
            }
    
            double averageReliability = totalReliability / totalCount;
            logger.info("Reducer: Vendor " + key + " has global average reliability: " + averageReliability);
    
            context.write(key, new DoubleWritable(averageReliability));
        }
    }
    
