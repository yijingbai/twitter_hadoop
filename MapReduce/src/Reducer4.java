import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: 0
// value: source,target edgeBetweenness

public class Reducer4 extends Reducer<Text, Text, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		double threshold = 2.0;
		double max = 0;
		
		while (iter.hasNext()) {
			String str[] = iter.next().toString().split(" |\\t|,");
			String source = str[0], target = str[1];
			double edgeBetweenness = Double.parseDouble(str[2]);
			
			if (edgeBetweenness > max && edgeBetweenness > threshold) {
				max = edgeBetweenness;
				Driver.edgesSelected = new HashMap<>();
				Driver.edgesSelected.put(source, new ArrayList<String>());
				Driver.edgesSelected.get(source).add(target);
			} else if (edgeBetweenness == max && edgeBetweenness > threshold) {
				if (Driver.edgesSelected.containsKey(source)) {
					Driver.edgesSelected.get(source).add(target);
				} else {
					if (Driver.edgesSelected.size() < Driver.mapSize) {
						Driver.edgesSelected.put(source, new ArrayList<String>());
						Driver.edgesSelected.get(source).add(target);
					}
				}
			}
		}
		
		for (String source: Driver.edgesSelected.keySet()) {
			for (String target: Driver.edgesSelected.get(source))
				try {
					context.write(keyText(source, target), new DoubleWritable(max));
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
	}
	
	public Text keyText(String source, String target) {
		StringBuilder sb = new StringBuilder();
		
		sb.append(source);
		sb.append(',');
		sb.append(target);
		
		return new Text(sb.toString());
	}
}
