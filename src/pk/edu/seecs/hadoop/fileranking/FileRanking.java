package pk.edu.seecs.hadoop.fileranking;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileRanking extends Configured implements Tool {
	
	static Log log = LogFactory.getLog(FileRanking.class);

	public static void main(String[] args) throws Exception {
		log.info("Starting File Ranking System");
		
		//Main function will call run method
        int res = ToolRunner.run(new Configuration(), new FileRanking(),args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception 
	{
        Job job = Job.getInstance();
        job.setInputFormatClass(CustomFileInputFormat.class); 
        job.setJobName("File Ranking");
        job.setMapperClass(FileRankingMapper.class);
        //job.setNumReduceTasks(0);
        job.setReducerClass(FileRankingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        Path inp = new Path(args[0]);
        Path out = new Path(args[1]);

        FileInputFormat.addInputPath(job, inp);
        FileOutputFormat.setOutputPath(job, out);
        
        return job.waitForCompletion(true) ? 0 : 1;
	}

}
