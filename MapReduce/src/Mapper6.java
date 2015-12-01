import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input text format: 
// targetId sourceId distance weight status pathList adjList

public class Mapper6 extends Mapper<Object, Text, Text, Text> {	
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String targetId = str[0];
		if (str.length > 1) {
			String[] adj = str[1].split(",");
			for (String user: adj) {
				try {
					context.write(new Text(targetId + "," + user), new Text());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
}
