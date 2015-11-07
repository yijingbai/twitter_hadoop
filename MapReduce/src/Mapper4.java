import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//input text format:
//source,target edgeBetweenness

public class Mapper4 extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) {
		try {
			context.write(new Text("0"), value);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
