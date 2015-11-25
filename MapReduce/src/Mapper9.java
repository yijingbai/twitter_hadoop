import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		
		HashMap<String,String> map = new HashMap<>();
		try {
			Path p = new Path("./communityNum");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(p);
			BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
			String line;
			while ((line = br.readLine()) != null) {
				String[] s = line.split(",");
				map.put(s[0], s[1]);
			}
			br.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		for (String user: users) {
			if (map.containsKey(user) && map.get(user).compareTo(communityNum) < 0)
				communityNum = map.get(user);
		}
		
		try {
			context.write(new Text(communityNum), new Text(str[1]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
