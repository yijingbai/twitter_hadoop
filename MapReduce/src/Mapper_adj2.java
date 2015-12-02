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
		HashMap<String, Set<String>> map = new HashMap<>();
		try {
			Path p = new Path("./selectedEdges");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(p);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String line;
			
			while ((line = br.readLine()) != null) {
				String[] s = line.split(",");
				if (!map.containsKey(s[0]))
					map.put(s[0], new HashSet<String>());
				map.get(s[0]).add(s[1]);
			}
			br.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println(map);
		
		String str[] = value.toString().split(" |\\t|,");
		StringBuilder sb = new StringBuilder();
		if (map.containsKey(str[0])) {
			for (int i = 1; i < str.length; i++) {
				if (!map.get(str[0]).contains(str[i])) {
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
