import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
			}
		}
		
		if (!unique) {
			try {
				context.write(key, new Text(minCommunityNum));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			try {
				Path p = new Path("./communityNum");
				FileSystem fs = FileSystem.get(context.getConfiguration());
				if (!fs.exists(p))
					fs.createNewFile(p);
				FSDataOutputStream out = fs.append(p);
				out.writeChars(key.toString() + "," + minCommunityNum + "\n");
				out.close();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}
}
