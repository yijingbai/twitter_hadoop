import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
	public static void main(String[] args) throws Exception {
		boolean allPathFound;
		boolean allCommunityFound;
		boolean isSelected;
		
		Path inputPath = new Path(args[0]);
		Path outputPath0 = new Path(args[1] + "/output0");
		Path outputPath_adj1 = new Path(args[1] + "/output_adj1");
		job0(inputPath, outputPath0);
		job_adj1(inputPath, outputPath_adj1);
		
		Path outputPath1 = new Path(args[1] + "/output1");
		Path outputPath2 = new Path(args[1] + "/output2");
		Path outputPath3 = new Path(args[1] + "/output3");
		Path outputPath4 = new Path(args[1] + "/output4");
		Path outputPath_adj2 = new Path(args[1] + "/output_adj2");
		Path temp;
		int outer = 1;
		while (true) {
			int inner = 1;
			allPathFound = false;
			while (!allPathFound) {
				allPathFound = job1(outputPath0, outputPath1, outputPath_adj1);
				temp = outputPath0;
				outputPath0 = outputPath1;
				outputPath1 = temp;
				
				System.out.println("finding shortest path: iteration #" + String.valueOf(inner));
				System.out.println(allPathFound);
				inner++;
			}
			
			job2(outputPath0, outputPath2);
			
			isSelected = job3(outputPath2, outputPath3);
			
			System.out.println("removing edges: iteration #" + String.valueOf(outer));
			System.out.println(isSelected);
			outer++;
			
			if (!isSelected)
				break;
			
			job4(outputPath0, outputPath4, outputPath3);
			job_adj2(outputPath_adj1, outputPath_adj2, outputPath3);
			temp = outputPath0;
			outputPath0 = outputPath4;
			outputPath4 = temp;
			temp = outputPath_adj1;
			outputPath_adj1 = outputPath_adj2;
			outputPath_adj2 = temp;
		}
		
		// job6 is to generate output for visualization (after detecting community)
		Path outputPath5 = new Path(args[1] + "/output5");
		job5(outputPath_adj1, outputPath5);
		
//		// start to group users into community
//		Path outputPath6 = new Path(args[1] + "/output6");
//		job6(outputPath_adj1, outputPath6);
//		
//		Path outputPath7 = new Path(args[1] + "/output7");
//		Path outputPath8 = new Path(args[1] + "/output8");
//		allCommunityFound = false;
//		while (!allCommunityFound) {
//			allCommunityFound = job7(outputPath6, outputPath7);
//			job8(outputPath6, outputPath8);
//			temp = outputPath6;
//			outputPath6 = outputPath8;
//			outputPath8 = temp;
//		}
//		
//		Path outputPath9 = new Path(args[1] + "/output9");
//		job9(outputPath6, outputPath9);
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
	
	private static void job_adj1(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "generating adjList");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper_adj1.class);
		job.setReducerClass(Reducer_adj1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
		
		FileStatus[] files = fs.listStatus(outputPath);
		List<Path> paths = new ArrayList<>();
		Path p = new Path(outputPath.toString() + "/adjList");
		for (int i = 0; i < files.length; i++) {
			if (files[i].getLen() != 0)
				paths.add(files[i].getPath());
		}
		fs.delete(p);
		fs.createNewFile(p);
		if (paths.size() > 0)
			fs.concat(p, paths.toArray(new Path[paths.size()]));
	}
	
	private static boolean job1(Path inputPath, Path outputPath, Path outputPath_adj) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		Path p = new Path(outputPath_adj.toString() + "/adjList");
		FSDataInputStream in = fs.open(p);
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));	
		String line;			
		while ((line = br.readLine()) != null) {
			String[] users = line.split(" |\\t");
			if (users.length > 1)
				conf.set(users[0], users[1]);
		}
		br.close();
		
		conf.set("pathname", outputPath.toString() + "/notAllPathFound");
		Job job = Job.getInstance(conf, "finding shortest paths");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
		
		p = new Path(outputPath.toString() +"/notAllPathFound");
		boolean allPathFound = !fs.exists(p);
		fs.delete(p);
		return allPathFound;
	}
	
	private static void job2(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "calculating edge betweenness");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
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
	
	private static boolean job3(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "selecting edges to be removed");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper3.class);
		job.setReducerClass(Reducer3.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
		
		FileStatus[] files = fs.listStatus(outputPath);
		List<Path> paths = new ArrayList<>();
		Path p = new Path(outputPath.toString() + "/selectedEdges");
		for (int i = 0; i < files.length; i++) {
			if (files[i].getLen() != 0)
				paths.add(files[i].getPath());
		}
		fs.delete(p);
		fs.createNewFile(p);
		if (paths.size() > 0)
			fs.concat(p, paths.toArray(new Path[paths.size()]));
		return (fs.getFileStatus(p).getLen() > 0);
	}
	
	private static void job4(Path inputPath, Path outputPath, Path outputPath_edges) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		Path p = new Path(outputPath_edges.toString() + "/selectedEdges");
		FSDataInputStream in = fs.open(p);
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));	
		String line;			
		while ((line = br.readLine()) != null) {
			String[] users = line.split(" |\\t|,");
			conf.set(users[0], users[1]);
		}
		br.close();
		
		Job job = Job.getInstance(conf, "removing edges in pathList");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper4.class);
		job.setReducerClass(Reducer4.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job_adj2(Path inputPath, Path outputPath, Path outputPath_edges) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		Path p = new Path(outputPath_edges.toString() + "/selectedEdges");
		FSDataInputStream in = fs.open(p);
		BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
		HashMap<String, Set<String>> map = new HashMap<>();
		String line;
		while ((line = br.readLine()) != null) {
			String[] pair = line.split(" |\\t|,");
			if (!map.containsKey(pair[0]))
				map.put(pair[0], new HashSet<String>());
			map.get(pair[0]).add(pair[1]);
		}
		br.close();
		
		for (String source: map.keySet()) {
			StringBuilder sb = new StringBuilder();
			for (String user: map.get(source)) {
				sb.append(user);
				sb.append(',');
			}
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1);
			conf.set(source, sb.toString());
		}
		
		Job job = Job.getInstance(conf, "removing edges in adjList");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper_adj2.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
		
		FileStatus[] files = fs.listStatus(outputPath);
		List<Path> paths = new ArrayList<>();
		p = new Path(outputPath.toString() + "/adjList");
		for (int i = 0; i < files.length; i++) {
			if (files[i].getLen() != 0)
				paths.add(files[i].getPath());
		}
		fs.delete(p);
		fs.createNewFile(p);
		if (paths.size() > 0)
			fs.concat(p, paths.toArray(new Path[paths.size()]));
	}
	
	private static void job5(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "generating output for visualization");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper5.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
	}
	
	private static void job6(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		conf.setLong("communityNum", 0);
		Job job = Job.getInstance(conf, "adding communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper6.class);
		job.setReducerClass(Reducer6.class);
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
	
	private static boolean job7(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "selecting the smallest communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper7.class);
		job.setReducerClass(Reducer7.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem fs = FileSystem.get(new URI(outputPath.toString()), conf);
		fs.delete(outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		System.out.println(job.waitForCompletion(true) ? "Success" : "Fail");
		Path p = new Path("./communityNum");
		boolean allCommunityFound = !fs.exists(p);
		return allCommunityFound;
	}
	
	private static void job8(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "updating communityNum");
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
		Path p = new Path("./communityNum");
		fs.delete(p);
	}
	
	private static void job9(Path inputPath, Path outputPath) throws Exception {
		Configuration conf = new Configuration();
		conf.setLong("communityNum", 0);
		Job job = Job.getInstance(conf, "adding communityNum");
		job.setJarByClass(Driver.class);
		job.setMapperClass(Mapper9.class);
		job.setReducerClass(Reducer9.class);
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
}
