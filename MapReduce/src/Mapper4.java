import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Mapper4 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String targetId = str[0], sourceId = str[1];
		List<String> pathList = new ArrayList<String>();
		
		StringTokenizer st;
		st = new StringTokenizer(str[5].substring(1, str[5].length() - 1), ",");
		while (st.hasMoreTokens()) {
			pathList.add(st.nextToken());
		}
		
		boolean remove = false;
		for (int i = 0; i < pathList.size() - 1; i++) {
			String source = pathList.get(i);
			String target = pathList.get(i + 1);
			
			String s = context.getConfiguration().get(source);
			if (s != null) {
				Set<String> set = new HashSet<>();
				String[] users = s.split(",");
				for (String user: users)
					set.add(user);
			
				if (set.contains(target)) {
					try {
						context.write(new Text(sourceId), valueText(sourceId));
					} catch (Exception e) {
						e.printStackTrace();
					}
					remove = true;
					break;
				}
			}
		}
		
		if (!remove) {
			try {
				context.write(new Text(targetId), valueText(str));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public Text valueText(String[] str) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 1; i < str.length; i++) {
			sb.append(str[i]);
			sb.append(' ');
		}
		
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
		
		return new Text(sb.toString());
	}
}
