import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input format:
// community# target,adj1,adj2...

public class Mapper10 extends Mapper<Object, Text, LongWritable, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		long communityNum = context.getConfiguration().getLong("communityNum", 0) + 1;
		try {
			context.write(new LongWritable(communityNum), new Text(str[1]));
		} catch (Exception e) {
			e.printStackTrace();
		}
		context.getConfiguration().setLong("communityNum", communityNum);
	}
}
