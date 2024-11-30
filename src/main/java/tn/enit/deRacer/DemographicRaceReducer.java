    package tn.enit.deRacer;

    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.log4j.LogManager;
    import org.apache.log4j.Logger;

    import java.io.IOException;

    public class DemographicRaceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static final Logger logger = LogManager.getLogger(DemographicRaceReducer.class);

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;

            // Sum the values for each education level
            for (IntWritable val : values) {
                totalCount += val.get();
            }

            logger.info("Reducer: " + key + " count: " + totalCount);

            // Write the results to the context
            context.write(key, new IntWritable(totalCount));
        }
    }
