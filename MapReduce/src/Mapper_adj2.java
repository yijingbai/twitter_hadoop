import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Mapper_adj2 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String str[] = value.toString().split(" |\\t|,");
		StringBuilder sb = new StringBuilder();
		String s = context.getConfiguration().get(str[0]);
		if (s != null) {
			Set<String> set = new HashSet<>();
			String[] users = s.split(",");
			for (String user: users)
				set.add(user);
			
			for (int i = 1; i < str.length; i++) {
				if (!set.contains(str[i])) {
					sb.append(str[i]);
					sb.append(',');
				}
			}
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1);
			
			try {
				context.write(new Text(str[0]), new Text(sb.toString()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			for (int i = 1; i < str.length; i++) {
				sb.append(str[i]);
				sb.append(',');
			}
			if (sb.length() > 0)
				sb.deleteCharAt(sb.length() - 1);
			
			try {
				context.write(new Text(str[0]), new Text(sb.toString()));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
