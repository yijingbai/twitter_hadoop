import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import org.json.*;


public class TwitterDataExtract {

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	            JSONArray mentions;
	            BigInteger userid;
	            String line = value.toString();
	            try{
	                    JSONObject obj = new JSONObject(line);
	                    userid = obj.getJSONObject("user").getBigInteger("id");
	                    mentions = obj.getJSONObject("entities").getJSONArray("user_mentions");
	                    for (int j = 0; j < mentions.length(); j++){
	                    	BigInteger userMention = mentions.getJSONObject(j).getBigInteger("id");
	                    	context.write(new Text(userid.toString()), new Text(userMention.toString()));
	                    }
	            }catch(JSONException e){
	                e.printStackTrace();
	            }
	        }
    }
	
    public static class Reduce extends Reducer<Text,Text,NullWritable,Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            try{
            	List<String> pathList = new ArrayList<String>();
            	List<String> adjList = new ArrayList<String>();
                for(Text val : values){
                   adjList.add(val.toString());
                }
                
                context.write(NullWritable.get(), valueText(key.toString(), key.toString(), 0, 0, "active", pathList, adjList));
            }catch(JSONException e){
                e.printStackTrace();
            }
        }
        
        public Text valueText(String sourceId, String targetId, long distance, long weight, String status, List<String> pathList, List<String> adjList) {
    		StringBuilder sb = new StringBuilder();
    		
    		sb.append(sourceId);
    		sb.append(' ');
    		sb.append(targetId);
    		sb.append(' ');
    		sb.append(distance);
    		sb.append(' ');
    		sb.append(weight);
    		sb.append(' ');
    		sb.append(status);
    		sb.append(' ');
    		
    		sb.append('[');
    		for (int i = 0; i < pathList.size(); i++) {
    			sb.append(pathList.get(i));
    			if (i < pathList.size() - 1)
    				sb.append(',');
    		}
    		sb.append(']');
    		sb.append(' ');
    		
    		sb.append('[');
    		for (int i = 0; i < adjList.size(); i++) {
    			sb.append(adjList.get(i));
    			if (i < adjList.size() - 1)
    				sb.append(',');
    		}
    		sb.append(']');
    		
    		return new Text(sb.toString());
    	}
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: TwitterDataExtract <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "TwitterDataExtract");
        job.setJarByClass(TwitterDataExtract.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}