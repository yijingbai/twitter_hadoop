import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// key: 0
// value: source,target edgeBetweenness

public class Reducer3 extends Reducer<Text, Text, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<Text> value, Context context) {
		Iterator<Text> iter = value.iterator();
		double threshold = 100.0;
		
		int K = 10000;
		PriorityQueue<Pair> pq = new PriorityQueue<>(K, new PairComparator());
		
		while (iter.hasNext()) {
			String str[] = iter.next().toString().split(" |\\t|,");
			String source = str[0], target = str[1];
			double edgeBetweenness = Double.parseDouble(str[2]);
			
			if (edgeBetweenness > threshold) {
				Pair pair = new Pair(source, target, edgeBetweenness);
				if (pq.size() < K) {
					pq.add(pair);
				} else {
					if (edgeBetweenness > pq.peek().edgeBetweenness) {
						pq.poll();
						pq.add(pair);
					}
				}
			}
		}
		
		while (!pq.isEmpty()) {
			Pair pair = pq.poll();
			String s = pair.source;
			String t = pair.target;
			double e = pair.edgeBetweenness;
			
			try {
				context.write(keyText(s, t), new DoubleWritable(e));
			} catch (Exception e1) {
				e1.printStackTrace();
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

class Pair {
	public String source;
	public String target;
	public double edgeBetweenness;
	
	public Pair(String s, String t, double e) {
		source = s;
		target = t;
		edgeBetweenness = e;
	}
}

class PairComparator implements Comparator<Pair> {
	public int compare(Pair x, Pair y) {
		return Double.compare(x.edgeBetweenness, y.edgeBetweenness);
	}
}
