import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class GetAvg {

	public static class Map  
    		extends Mapper<LongWritable, Text, Text, DoubleWritable>{ 
	
		private DoubleWritable rating = new DoubleWritable();   // type of output key 
		private Text movieId= new Text();
		
		public void map(LongWritable key, Text value, Context context 
		    ) throws IOException, InterruptedException { 
			String[] line = value.toString().split("::"); // line to token 
			
			movieId.set(line[1]);
			rating.set(Double.parseDouble(line[2]));
			context.write(movieId, rating);     // create a pair <keyword, 1>  
			
		} 
	} 
	
	
	public static class Reduce 
			extends Reducer<Text,DoubleWritable,Text,DoubleWritable> { 
	
		private DoubleWritable result = new DoubleWritable(); 
		
		public void reduce(Text key, Iterable<DoubleWritable> values,  
		          Context context ) 
		        		  throws IOException, InterruptedException { 
			
			double sum = 0.0; // initialize the sum for each movie 
			int n = 0;
			for (DoubleWritable val : values) { 
				sum += val.get();
				n++;
			}
			double avg = Double.parseDouble(String.format("%04.3f", sum/n));
			result.set(avg); 
	
			context.write(key, result);
		} 
} 

}
