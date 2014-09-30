
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*public class MaxTemperatureReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : values) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(key, new IntWritable(maxValue));
	}
}*/
public class WordNetReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Set<String> neighbours = new HashSet<String>();
		for (Text value : values) {
			neighbours.add(value.toString());		
		} 
		String neighboursText = "";
		for ( String str : neighbours ) {
			neighboursText = neighboursText.concat(str).concat(" ");
		}
		//ArrayWritable array = new ArrayWritable(Text.class);
		//array.set(neighbours.toArray(new Text[neighbours.size()]));
		context.write(key, new Text(neighboursText));
	}

}