import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input text format: 
// targetId sourceId distance weight status pathList adjList

public class Mapper2 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		try {
			context.write(keyText(str), valueText(str));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Text keyText(String[] str) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(str[0]);
		sb.append(',');
		sb.append(str[1]);
		
		return new Text(sb.toString());
	}
	
	public Text valueText(String[] str) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 2; i < str.length; i++) {
			sb.append(str[i]);
			if (i != str.length - 1)
				sb.append(' ');
		}
		
		return new Text(sb.toString());
	}
}