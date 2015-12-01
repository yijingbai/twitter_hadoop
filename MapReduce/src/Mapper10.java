import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// input format:
// community# target,adj1,adj2...

public class Mapper10 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		String[] str = value.toString().split(" |\\t");
		
		try {
			context.write(new Text(str[1]), new Text());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
