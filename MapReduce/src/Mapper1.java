import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// input text format: 
// targetId sourceId distance weight status pathList adjList

public class Mapper1 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String targetId = str[0];
		String sourceId = str[1];
		long distance = Long.parseLong(str[2]);
		long weight = Long.parseLong(str[3]);
		String status = str[4];
		List<String> pathList = new ArrayList<String>();
		List<String> adjList = new ArrayList<String>();
		
		StringTokenizer st;
		st = new StringTokenizer(str[5].substring(1, str[5].length() - 1), ",");
		while (st.hasMoreTokens()) {
			pathList.add(st.nextToken());
		}
		
		st = new StringTokenizer(str[6].substring(1, str[6].length() - 1), ",");
		while (st.hasMoreTokens()) {
			adjList.add(st.nextToken());
		}
		
		if (status.equals("inactive")) {
			try {
				context.write(new Text(targetId), valueText(sourceId, distance, weight, status, pathList, adjList));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			status = "inactive";
			distance++;
			pathList.add(targetId);
			try {
				context.write(new Text(targetId), valueText(sourceId, distance, weight, status, pathList, adjList));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// extend to adjacent vertices
			for (String id: adjList) {
				status = "active";
				targetId = id;
				adjList = new ArrayList<String>();
				try {
					context.write(new Text(targetId), valueText(sourceId, distance, weight, status, pathList, adjList));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			Driver.allPathFound = false;
		}
	}
	
	public Text valueText(String sourceId, long distance, long weight, String status, List<String> pathList, List<String> adjList) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(sourceId);
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
