package pk.edu.seecs.hadoop.fileranking;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CustomFileRecordReader extends RecordReader<Text, Text> {
	
	Text key;
	boolean available = false;
	
	Log log = LogFactory.getLog(CustomFileRecordReader.class);

	@Override
	public void close() throws IOException {
		
	}

	@Override
	public Text getCurrentKey() throws IOException,
			InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	/**
     * From Design Pattern, O'Reilly...
     * This method takes as arguments the map task’s assigned InputSplit and
     * TaskAttemptContext, and prepares the record reader. For file-based input
     * formats, this is a good place to seek to the byte position in the file to
     * begin reading.
     */
	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		// This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) genericSplit;
 
        // Retrieve configuration, and Max allowed
        // bytes for a single record
        Configuration job = context.getConfiguration();
	
        // Retrieve file containing Split "S"
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        
        log.info(file.toString());
        
        //Check if the current item is a file
        if(fs.isFile(file))
        {        	
        	log.info("File Verified");
        	available = true;
        	key = new Text(file.toString());
        }
        else
        {
            available = false;
        }
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(available)
		{
			available = false;
			return true;
		}
		
		return false;
	}

}
