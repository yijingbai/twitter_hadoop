import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


// remove duplicate
public class Reducer5 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();	
		
		boolean emitted = false;
		while (iter.hasNext()) {
			String[] str = iter.next().toString().split(" ");
			if (str[4].equals("[]") && !emitted) {
				try {
					context.write(key, valueText(str));
				} catch (Exception e) {
					e.printStackTrace();
				}
				emitted = true;
			} else if (!str[4].equals("[]")){
				try {
					context.write(key, valueText(str));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public Text valueText(String[] str) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < str.length; i++) {
			sb.append(str[i]);
			sb.append(' ');
		}
		
		return new Text(sb.toString());
	}
}
