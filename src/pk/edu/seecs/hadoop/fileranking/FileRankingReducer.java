package pk.edu.seecs.hadoop.fileranking;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FileRankingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	Log log = LogFactory.getLog(FileRankingMapper.class);

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		log.info("Starting parsing values");
		for(IntWritable current : values)
        {
			log.info("Values: " + current.get());
			sum += current.get();
        }
        
		//output.collect(key, new IntWritable(sum));
		context.write(key, new IntWritable(sum));
	}

}
