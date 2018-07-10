import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.GenericOptionsParser;


public class Topten {

	public static class Map  
    extends  Mapper<Object, Text, NullWritable, Text> {
    	  // Stores a map of user reputation to the record
    	  private HashMap<Text,Double> repToRecordMap = new HashMap<Text,Double>();
    	  private double min = 5.1;
    	  private Text minkey = new Text(); 
    	  public void map(Object key, Text value, Context context)
    	      throws IOException, InterruptedException {
    	    
    		  String[] line = value.toString().split("\t");
    		  //String movieId = line[0];
	    	  double rating = Double.parseDouble(line[1]);
	    	    // Add this record to our map with the reputation as the key
	    	    repToRecordMap.put(new Text(value),rating);
	    	    // If we have more than ten records, remove the one with the lowest rep
	    	    // As this tree map is sorted in descending order, the user with
	    	    // the lowest reputation is the last key.
	    	    if (repToRecordMap.size() == 10){
	    	    	findMin();
	    	    }
	    	    if (repToRecordMap.size() > 10) {
	    	    	if (rating>min){
	    	    		repToRecordMap.remove(minkey);
	    	    		min = 5.1;
	    	    		findMin();
	    	    	}
	    	    	else
	    	    		repToRecordMap.remove(new Text(value));
	    	    }
	    	    	 
	      }
	    	  
	    	  private void findMin() {
	    		  for (Text key : repToRecordMap.keySet()) {
	    			    if (repToRecordMap.get(key)<min){
	    			    	min = repToRecordMap.get(key);
	    			    	minkey.set(key);
	    			    }
	    		  }
	    		  
	    	  }

			protected void cleanup(Context context) throws IOException,
	    	      InterruptedException {
	    	    // Output our ten records to the reducers with a null key
	    	    for (Text t : repToRecordMap.keySet()) {
	    	      context.write(NullWritable.get(), t);
	    	    }
	    	  }
	    	  
	} 
	public static class Reduce 
			extends  Reducer<NullWritable, Text, NullWritable, Text> {
		  // Stores a map of user reputation to the record
		  // Overloads the comparator to order the reputations in descending order
		private HashMap<Text,Double> repToRecordMap = new HashMap<Text,Double>();
		
		private double min = 5.1;
  	  	private Text minkey = new Text(); 
		
		  public void reduce(NullWritable key, Iterable<Text> values,
		      Context context) throws IOException, InterruptedException {
		    for (Text value : values) {
		    	
		    	String[] line = value.toString().split("\t");
	    		
	    		  //String movieId = line[0];
		    	  double rating = Double.parseDouble(line[1]);
		    	    // Add this record to our map with the reputation as the key
		    	    repToRecordMap.put(new Text(value),rating);
		      // If we have more than ten records, remove the one with the lowest rep
		      // As this tree map is sorted in descending order, the user with
		      // the lowest reputation is the last key.
		    	    if (repToRecordMap.size() == 10){
		    	    	findMin();
		    	    }
		    	    if (repToRecordMap.size() > 10) {
		    	    	if (rating>min){
		    	    		repToRecordMap.remove(minkey);
		    	    		min = 5.1;
		    	    		findMin();
		    	    	}
		    	    	else
		    	    		repToRecordMap.remove(new Text(value));
		    	    }
		    }
		    
		    
		    for (Text t : repToRecordMap.keySet()) {
			      context.write(NullWritable.get(), t);
			    }
		   
		    
		    
		  }
		  
		 
		  
		  private void findMin() {
    		  for (Text key : repToRecordMap.keySet()) {
    			    if (repToRecordMap.get(key)<min){
    			    	min = repToRecordMap.get(key);
    			    	minkey.set(key);
    			    }
    		  }
    		  
    	  }
	}

 

//Driver program 
	public static void main(String[] args) throws Exception { 
		Configuration conf = new Configuration();  
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); 
		//get all args 
		if (otherArgs.length != 3) { 
		System.err.println("Usage: Topten <in> <out1> <out2>"); 
		System.exit(2); 
		} 
		
		
		
		// create a job with name "getavg" 
		Job avgJob = new Job(conf, "getavg");
		//JobConf avgJob = new JobConf();
		
		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(avgJob);
		
		avgJob.setJarByClass(GetAvg.class); 
		avgJob.setMapperClass(GetAvg.Map.class); 
		avgJob.setReducerClass(GetAvg.Reduce.class); 
		
		// uncomment the following line to add the Combiner 
		//job.setCombinerClass(Reduce.class); 
		
		// set output key type    
		avgJob.setOutputKeyClass(Text.class); 
		// set output value type 
		avgJob.setOutputValueClass(DoubleWritable.class); 
		//set the HDFS path of the input data 
		FileInputFormat.addInputPath(avgJob, new Path(otherArgs[0])); 
		// set the HDFS path for the output 
		FileOutputFormat.setOutputPath(avgJob, new Path(otherArgs[1])); 
		
		
		
		Job topTenJob = new Job(conf, "topten");
		//JobConf topTenJob = new JobConf();
		ControlledJob cJob2 = new ControlledJob(conf);
		cJob2.setJob(topTenJob);
		
		topTenJob.setJarByClass(Topten.class); 
		topTenJob.setMapperClass(Map.class); 
		topTenJob.setReducerClass(Reduce.class);
		
		//set output key type    
		topTenJob.setOutputKeyClass(NullWritable.class); 
		//set output value type 
		topTenJob.setOutputValueClass(Text.class); 
		//set the HDFS path of the input data
		
		FileInputFormat.addInputPath(topTenJob, new Path(otherArgs[1])); 
		//set the HDFS path for the output 
		FileOutputFormat.setOutputPath(topTenJob, new Path(otherArgs[2]));
		
		JobControl jctrl = new JobControl("jctrl");
		
		jctrl.addJob(cJob1);
		jctrl.addJob(cJob2);
		cJob2.addDependingJob(cJob1);
		//jctrl.run();
		
//		Thread t = new Thread(jctrl);
//		t.start();
//		while(true){    
//            if(jctrl.allFinished()){    
//                System.out.println(jctrl.getSuccessfulJobList());    
//                jctrl.stop();
//                System.exit(0);
//                break;    
//            }
//            
//		}
//		while(!jctrl.allFinished()){
//			
//		}
//		System.out.println(jctrl.getSuccessfulJobList());    
//        jctrl.stop();
//        System.exit(0);
        
		//Wait till job completion 
			if(avgJob.waitForCompletion(true)){
				System.exit(topTenJob.waitForCompletion(true) ? 0 : 1);
			}
		
		//System.exit(jctrl.allFinished() ? 0 : 1); 
		
		
	}
}
