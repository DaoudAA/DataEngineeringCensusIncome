    package tn.enit.de;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.io.DoubleWritable;
    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.log4j.LogManager;
    import org.apache.log4j.Logger;

    import java.io.IOException;

    public class AvgSalaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private static final Logger logger = LogManager.getLogger(AvgSalaryReducer.class);
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalSalary = 0;
            int totalCount = 0;

            for (DoubleWritable val : values) {
                totalSalary += val.get();
                totalCount++;
            }
            //System.out.println("Combiner : Processing line: count of  " + key +" is  : "+ totalCount);
            double averageSalary = totalSalary / totalCount;
            logger.info("REducerr : Processing line: " + key +" has averagehours of work : "+ averageSalary);
            context.write(key, new DoubleWritable(averageSalary));
        }
    }
