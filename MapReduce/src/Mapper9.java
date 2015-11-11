import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input format:
// community# user1,user2,user3...

// output format: (update community#)
// community# user1,user2,user3...

public class Mapper9 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		String communityNum = str[0];
		String[] users = str[1].split(",");
		
		for (String user: users) {
			if (Driver.communityBelonged.containsKey(user) && Driver.communityBelonged.get(user).compareTo(communityNum) < 0)
				communityNum = Driver.communityBelonged.get(user);
				
		}
		
		try {
			context.write(new Text(communityNum), new Text(str[1]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
