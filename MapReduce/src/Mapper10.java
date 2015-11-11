import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input format:
// community# target,adj1,adj2...

public class Mapper10 extends Mapper<Object, Text, LongWritable, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		Driver.communityNum++;
		try {
			context.write(new LongWritable(Driver.communityNum), new Text(str[1]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
