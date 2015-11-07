import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MarkableIterator;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class Reducer5 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		MarkableIterator<Text> iter = new MarkableIterator<Text>(value.iterator());	
		String adjListStr = "[]";
		
		try {
			iter.mark();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (iter.hasNext()) {
			String[] str = iter.next().toString().split(" ");
			if (!str[5].equals("[]")) {
				adjListStr = str[5];
				break;
			}
		}
		
		try {
			iter.reset();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		boolean emitted = false;
		while (iter.hasNext()) {
			Text text = iter.next();
			String[] str = text.toString().split(" ");
			if (key.toString().equals(str[0])) {
				if (!emitted) {
					try {
						context.write(key, valueText(str, adjListStr));
					} catch (Exception e) {
						e.printStackTrace();
					}
					emitted = true;
				}
			} else {
				try {
					context.write(key, text);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public Text valueText(String[] str, String adjListStr) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i < str.length - 1; i++) {
			sb.append(str[i]);
			sb.append(' ');
		}
		sb.append(adjListStr);
		
		return new Text(sb.toString());
	}
}
