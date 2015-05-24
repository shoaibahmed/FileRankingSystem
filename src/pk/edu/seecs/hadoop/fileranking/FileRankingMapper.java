package pk.edu.seecs.hadoop.fileranking;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FileRankingMapper extends Mapper<Text, Text, Text, IntWritable> {
	
	Log log = LogFactory.getLog(FileRankingMapper.class);

	public void map(Text key, Text value, Context context
            ) throws IOException, InterruptedException {
		
		//Extract file information from HDFS
		Path path = new Path(key.toString());
		
		Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        FileSystem fs = FileSystem.get(conf);
        
        /*
        FileStatus[] status = fs.listStatus(new Path("./input/"));
        for(int i=0;i<status.length;i++){
            log.info(status[i].getPath());
        }*/
        
        FileStatus[] status = fs.listStatus(path);
        for(int i=0;i<status.length;i++)
        {
        	log.info("File Details: " + status[i].toString());
        	
        	//Add last access time weightage
        	long accessTime = status[i].getAccessTime();
            context.write(key, getLastAccessTimeWeightage(accessTime));

            //Add last modified time weightage
        	long modifiedTime = status[i].getModificationTime();
            context.write(key, getLastModifiedTimeWeightage(modifiedTime));
            
        	//Add file replication weightage
            short replication = status[i].getReplication();
            context.write(key, getFileReplicationWeightage(replication));
        	
            //Add file owner weightage
            String owner = status[i].getOwner();
            context.write(key, getOwnerWeightage(owner));

            //Add user group weightage
            String userGroup = status[i].getGroup();
            context.write(key, getUserGroupWeightage(userGroup));
            
            //Add file size weightage
            long fileSize = status[i].getLen();
            context.write(key, getFileSizeWeightage(fileSize));
            
            //Add symbolic link weightage (Indicates that this file is being referred)
            boolean isSymLink = status[i].isSymlink();
            context.write(key, getSymbolicLinkWeightage(isSymLink));
            
            //Add Encrypted file weightage (Encryption indicates important file)
            boolean isEncrypted = status[i].isEncrypted();
            context.write(key, getEncryptedWeightage(isEncrypted));
            
            //Add file extension weightage
            String fileName = status[i].getPath().toString();
        	int extIndex = fileName.lastIndexOf('.');
        	
            if(extIndex != -1)
            {
            	String extension = fileName.substring(extIndex + 1);
            	context.write(key, getExtensionWeightage(extension));
            }
        }
		
		//context.write(key, value);
	}
	
	
	public IntWritable getLastAccessTimeWeightage(long accessTime)
	{
		//Convert given access in ms to seconds
		accessTime = accessTime / 1000;
		
		//Convert the access time in seconds to hours
		int accessTimeInHrs = (int) (accessTime / 3600);
		
		return new IntWritable(accessTimeInHrs);
	}
	
	
	public IntWritable getLastModifiedTimeWeightage(long modifiedTime)
	{
		//Convert given access in ms to seconds
		modifiedTime = modifiedTime / 1000;
		
		//Convert the access time in seconds to hours
		int modifiedTimeInHrs = (int) (modifiedTime / 3600);
		
		return new IntWritable(modifiedTimeInHrs);
	}
	
	
	public IntWritable getFileReplicationWeightage(short replication)
	{
		return new IntWritable(replication);
	}
	
	
	public IntWritable getOwnerWeightage(String owner)
	{
		return new IntWritable(0);
	}
	
	
	public IntWritable getUserGroupWeightage(String userGroup)
	{
		log.info("User Group: " + userGroup);
		if(userGroup.equals("supergroup"))
			return new IntWritable(1);
		else
			return new IntWritable(0);
	}
	
	
	public IntWritable getExtensionWeightage(String extension)
	{
		log.info("File extension: " + extension);
		//If system file
		if(extension.equals("dll") || extension.equals("so") || extension.equals("lib"))
		{
			return new IntWritable(2);
		}
		//If code file
		else if(extension.equals("c") || extension.equals("h") || extension.equals("m") || extension.equals("cpp") || extension.equals("hpp"))
		{
			return new IntWritable(1);
		}
		//If none found
		else
		{
			return new IntWritable(0);
		}
	}
	
	
	public IntWritable getSymbolicLinkWeightage(boolean isSymLink)
	{
		if(isSymLink)
			return new IntWritable(1);
		else
			return new IntWritable(0);
	}
	
	
	public IntWritable getEncryptedWeightage(boolean isEncrypted)
	{
		if(isEncrypted)
			return new IntWritable(1);
		else
			return new IntWritable(0);
	}
	
	
	public IntWritable getFileSizeWeightage(long fileSize)
	{
		long GIGA = 1073741824;
		
		//Convert given file size in bytes to GB
		int fileSizeInGB = (int) (fileSize / GIGA);
		
		return new IntWritable(fileSizeInGB);
	}

}
