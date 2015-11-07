import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Mapper5 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String targetId = str[0];
		String sourceId = str[1];
		List<String> adjList = new ArrayList<String>();
		
		StringTokenizer st;
		st = new StringTokenizer(str[6].substring(1, str[6].length() - 1), ",");
		while (st.hasMoreTokens()) {
			adjList.add(st.nextToken());
		}
		
		if (Driver.edgesSelected.containsKey(targetId)) {
			List<String> removeList = Driver.edgesSelected.get(targetId);
			for (String s: removeList) {
				adjList.remove(s);
			}
			try {
				context.write(new Text(targetId), valueText(str, adjList));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (Driver.edgesSelected.containsKey(sourceId)) {
			try {
				context.write(new Text(sourceId), valueText(sourceId));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			try {
				context.write(new Text(targetId), valueText(str));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public Text valueText(String[] str, List<String> adjList) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 1; i < str.length - 1; i++) {
			sb.append(str[i]);
			sb.append(' ');
		}
		
		sb.append('[');
		for (int i = 0; i < adjList.size(); i++) {
			sb.append(adjList.get(i));
			if (i < adjList.size() - 1)
				sb.append(',');
		}
		sb.append(']');
		
		return new Text(sb.toString());
	}
	
	public Text valueText(String sourceId) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(sourceId);
		sb.append(' ');
		sb.append("0");
		sb.append(' ');
		sb.append("1");
		sb.append(' ');
		sb.append("active");
		sb.append(' ');
		sb.append('[');
		sb.append(']');
		sb.append(' ');
		sb.append('[');
		sb.append(']');
		
		return new Text(sb.toString());
	}
	
	public Text valueText(String[] str) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 1; i < str.length; i++) {
			sb.append(str[i]);
			if (i < str.length - 1)
				sb.append(' ');
		}
		
		return new Text(sb.toString());
	}
}
