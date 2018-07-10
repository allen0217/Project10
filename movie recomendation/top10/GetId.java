import java.io.IOException; 
 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; 
import org.apache.hadoop.util.GenericOptionsParser; 
 
public class GetId { 
	 
	//private static String zip;
	  public static class Map  
	            extends Mapper<LongWritable, Text, NullWritable, Text>{ 
		  
		   
	      
	    private Text userid = new Text();   // type of output key 
	    
	    public void map(LongWritable key, Text value, Context context 
	            ) throws IOException, InterruptedException { 
			String[] line = value.toString().split("::"); // line to token 
			Configuration conf = context.getConfiguration();
			String zip = conf.get("zipcode");
			
			if (line[4].equals(zip)){
				userid.set(line[0]);
				context.write(NullWritable.get(), userid);     // create a pair <keyword, 1>  
			}
	    } 
	} 
	  public static class Reduce 
	  extends Reducer<NullWritable,Text,NullWritable,Text> { 
	
	
	public void reduce(NullWritable key, Iterable<Text> values,  
	                  Context context 
	                  ) throws IOException, InterruptedException { 
	 for (Text val : values) { 
		 context.write(key,val); 
	 } 
	
	  
	} 
	} 

// Driver program 
	public static void main(String[] args) throws Exception { 
	Configuration conf = new Configuration();  
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
	//get all args 
	if (otherArgs.length != 3) { 
	 System.err.println("Usage: WordCount <zip> <out>"); 
	 System.exit(2); 
	 
	} 
//	set the input zipcode
	conf.set("zipcode",otherArgs[0]);
//	zip = otherArgs[0];
	// create a job with name "getid" 
	Job job = new Job(conf,"getid"); 
	job.setJarByClass(GetId.class); 
	job.setMapperClass(Map.class); 
	job.setReducerClass(Reduce.class); 
	
	// uncomment the following line to add the Combiner 
	//job.setCombinerClass(Reduce.class); 
	 
	
	// set output key type   
	job.setOutputKeyClass(NullWritable.class); 
	// set output value type 
	job.setOutputValueClass(Text.class); 
	
	 //set the HDFS path of the input data 
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	// set the HDFS path for the output 
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[2])); 
	
	//Wait till job completion 
	System.exit(job.waitForCompletion(true) ? 0 : 1); 
} 
}  