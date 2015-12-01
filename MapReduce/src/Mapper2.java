import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input text format: 
// targetId sourceId distance weight status pathList adjList

public class Mapper2 extends Mapper<Object, Text, Text, DoubleWritable> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String path = str[5].substring(1, str[5].length() - 1);
		String[] users = path.split(",");
		long weight = Long.parseLong(str[3]);
		
		for (int i = 0; i < users.length - 1; i++) {
			try {
				context.write(keyText(users[i], users[i + 1]), new DoubleWritable(1.0 / weight));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public Text keyText(String source, String target) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(source);
		sb.append(',');
		sb.append(target);
		
		return new Text(sb.toString());
	}
}
