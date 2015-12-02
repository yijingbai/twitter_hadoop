import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		
		try {
			String pathname = context.getConfiguration().get("pathname");
			Path p = new Path(pathname);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(p);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String line;			
			while ((line = br.readLine()) != null) {
				String[] users = line.split(" |\\t|,");
				if (users[0].equals(targetId)) {
					for (int i = 1; i < users.length; i++)
						adjList.add(users[i]);
					break;
				}
			}
			br.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//System.out.println(adjList);
		
		if (status.equals("inactive")) {
			try {
				context.write(keyText(targetId, sourceId), valueText(distance, weight, status, pathList));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			status = "inactive";
			distance++;
			pathList.add(targetId);
			try {
				context.write(keyText(targetId, sourceId), valueText(distance, weight, status, pathList));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// extend to adjacent vertices
			if (pathList.size() < 4) {
				for (String id: adjList) {
					status = "active";
					targetId = id;
					adjList = new ArrayList<String>();
					try {
						context.write(keyText(targetId, sourceId), valueText(distance, weight, status, pathList));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				
				try {
					FileSystem fs = FileSystem.get(context.getConfiguration());
					fs.createNewFile(new Path("./notAllPathFound"));
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
	}
	
	public Text keyText(String targetId, String sourceId) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(targetId);
		sb.append(',');
		sb.append(sourceId);
		
		return new Text(sb.toString());
	}
	
	public Text valueText(long distance, long weight, String status, List<String> pathList) {
		StringBuilder sb = new StringBuilder();
		
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
		
		return new Text(sb.toString());
	}
}
