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
	public static boolean allCommunityFound;
	public static long communityNum;
	public static int mapSize = 1024;  // fixed size for hash map
	public static HashMap<String, List<String>> edgesSelected = new HashMap<>();	
	public static HashMap<String, String> communityBelonged;  //<user, community#>
	
	public static void main(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outputPath0 = new Path(args[1] + "/output0");
		job0(inputPath, outputPath0);
		
		while (true) {
			Path outputPath1 = new Path(args[1] + "/output1");
			Path outputPath2 = new Path(args[1] + "/output2");
			allPathFound = false;
			while (!allPathFound) {
				allPathFound = true;
				job1(outputPath0, outputPath1);
				job2(outputPath1, outputPath2);
				outputPath0 = outputPath2;
			}
			
			Path outputPath3 = new Path(args[1] + "/output3");
			job3(outputPath2, outputPath3);
			
			Path outputPath4 = new Path(args[1] + "/output4");
			job4(outputPath3, outputPath4);
			
			if (edgesSelected.isEmpty())
				break;
			
			Path outputPath5 = new Path(args[1] + "/output5");
			job5(outputPath2, outputPath5);
			outputPath0 = outputPath5;
		}
		
		// job6 is to generate output for visualization (after detecting community)
		Path outputPath6 = new Path(args[1] + "/output6");
		job6(outputPath0, outputPath6);
		
		// start to group users into community
		Path outputPath7 = new Path(args[1] + "/output7");
		communityNum = 0;
		job7(outputPath0, outputPath7);
		
		Path outputPath8 = new Path(args[1] + "/output8");
		Path outputPath9 = new Path(args[1] + "/output9");
		allCommunityFound = false;
		while (!allCommunityFound) {
			allCommunityFound = true;
			communityBelonged = new HashMap<String, String>();
			job8(outputPath7, outputPath8);
			job9(outputPath7, outputPath9);
			Path temp = outputPath7;
			outputPath7 = outputPath9;
			outputPath9 = temp;
		}
		
		Path outputPath10 = new Path(args[1] + "/output10");
		communityNum = 0;
		job10(outputPath7, outputPath10);
	}
	
	private static void job0(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "generating input format");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper0.class);
		job.setReducerClass(Reducer0.class);
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
		Job job = Job.getInstance(conf, "generating output for visualization");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper6.class);
		//job.setReducerClass(Reducer6.class);
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
	
	private static void job7(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "adding communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper7.class);
		job.setReducerClass(Reducer7.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job8(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "selecting the smallest communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper8.class);
		job.setReducerClass(Reducer8.class);
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
	
	private static void job9(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "updating communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper9.class);
		job.setReducerClass(Reducer9.class);
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
	
	private static void job10(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "adding communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper10.class);
		//job.setReducerClass(Reducer10.class);
		job.setMapOutputKeyClass(LongWritable.class);
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
