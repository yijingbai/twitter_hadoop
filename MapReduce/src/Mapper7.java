import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input format:
// community# user1,user2,user3...

// output format:
// user community#

public class Mapper7 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		String[] users = str[1].split(",");
		
		for (String user: users) {
			try {
				context.write(new Text(user), new Text(str[0]));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
