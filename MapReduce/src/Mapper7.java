import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input text format: 
// targetId sourceId distance weight status pathList adjList

// output format:
// community# target,adj1,adj2...

public class Mapper7 extends Mapper<Object, Text, Text, Text> {	
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String targetId = str[0];
		String[] adj = str[6].substring(1, str[6].length() - 1).split(",");
		
		try {
			context.write(keyText(targetId, adj), new Text());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Text keyText(String targetId, String[] adj) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(targetId);
		for (String user: adj) {
			sb.append(',');
			sb.append(user);
		}
		
		return new Text(sb.toString());
	}
}
