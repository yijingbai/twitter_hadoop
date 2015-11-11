import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: user
// value: community#

public class Reducer8 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		boolean unique = true;
		
		String minCommunityNum = iter.next().toString();
		while (iter.hasNext()) {
			String communityNum = iter.next().toString();
			if (communityNum.compareTo(minCommunityNum) < 0) {
				minCommunityNum = communityNum;
				unique = false;
				Driver.allCommunityFound = false;
			}
		}
		
		if (Driver.communityBelonged.size() < Driver.mapSize && !unique) {
			Driver.communityBelonged.put(key.toString(), minCommunityNum);
		}
		
		try {
			context.write(key, new Text(minCommunityNum));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
