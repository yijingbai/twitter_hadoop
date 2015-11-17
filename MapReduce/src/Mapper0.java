import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input format: user1,user2	// user1 following user2
// key: user1
// value: user2

public class Mapper0 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(",");
		String targetId = str[0];
		String followingId = str[1];
		
		try {
			context.write(new Text(targetId), new Text(followingId));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
