    package tn.enit.de;
    import org.apache.hadoop.mapreduce.Reducer;
    import org.apache.hadoop.io.DoubleWritable;
    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;

    import java.io.IOException;

    public class AvgSalaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalSalary = 0;
            int totalCount = 0;

            for (DoubleWritable val : values) {
                totalSalary += val.get();
                totalCount++;
            }
            System.out.println("Combiner : Processing line: count of  " + key +" is  : "+ totalCount);
            double averageSalary = totalSalary / totalCount;
            context.write(key, new DoubleWritable(averageSalary));
        }
    }
