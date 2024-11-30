package tn.enit.deRacer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class DemographicRaceMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text race = new Text();
    private static final Logger logger = LogManager.getLogger(DemographicRaceMapper.class);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 8) {
            race.set(fields[8].trim());

            context.write(new Text(race), new IntWritable(1));
            //logger.info("MAPPER: Processing line: " + maritalStatus +" has this salary : "+ salary.toString());
        }
    }
}
