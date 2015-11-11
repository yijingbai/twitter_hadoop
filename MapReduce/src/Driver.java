import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Driver {
	public static boolean allPathFound;
	public static HashMap<String, List<String>> edgesSelected;
	
	public static void main(String[] args) throws Exception {
		Path inputPath = new Path("/Users/dannywang/hadoop-2.7.1/input");
		// need a loop
			Path outputPath1 = new Path("/Users/dannywang/hadoop-2.7.1/output1");
			Path outputPath2 = new Path("/Users/dannywang/hadoop-2.7.1/output2");
			allPathFound = false;
			while (!allPathFound) {
				allPathFound = true;
				job1(inputPath, outputPath1);
				job2(outputPath1, outputPath2);
				inputPath = outputPath2;
			}
			
			Path outputPath3 = new Path("/Users/dannywang/hadoop-2.7.1/output3");
			job3(outputPath2, outputPath3);
			
			Path outputPath4 = new Path("/Users/dannywang/hadoop-2.7.1/output4");
			job4(outputPath3, outputPath4);
			
			Path outputPath5 = new Path("/Users/dannywang/hadoop-2.7.1/output5");
			job5(outputPath2, outputPath5);
			inputPath = outputPath5;
			
		Path outputPath6 = new Path("Users/dannywang/hadoop-2.7.1/output6");
		job6(outputPath5, outputPath6);
	}
	
	private static void job1(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "building path");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job2(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "finding shortest path");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job3(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calculating edge betweenness");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper3.class);
		job.setReducerClass(Reducer3.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job4(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "selecting edges to be removed");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper4.class);
		job.setReducerClass(Reducer4.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job5(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "removing edges selected");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper5.class);
		job.setReducerClass(Reducer5.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job6(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "generating output");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper5.class);
		//job.setReducerClass(Reducer5.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
}
