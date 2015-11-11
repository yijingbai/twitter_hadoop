import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: community#
// value: user1,user2,user3...

public class Reducer9 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		HashSet<String> set = new HashSet<String>();
		
		while (iter.hasNext()) {
			String[] users = iter.next().toString().split(",");
			for (String user: users)
				set.add(user);
		}
		
		try {
			context.write(key, valueText(set));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Text valueText(HashSet<String> set) {
		StringBuilder sb = new StringBuilder();
		
		for (String user: set) {
			sb.append(user);
			sb.append(',');
		}
		sb.deleteCharAt(sb.length() - 1);
		
		return new Text(sb.toString());
	}
}
