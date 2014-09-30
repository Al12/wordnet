
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

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
	private HTable table;
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
		byte[] row = Bytes.toBytes(key.toString());
		Put p = new Put(row);
		p.add(Bytes.toBytes("neighbours"), Bytes.toBytes("list"), Bytes.toBytes(neighboursText));
		table.put(p);
	}
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration config = HBaseConfiguration.create(context.getConfiguration());				
		this.table = new HTable(config, "neighbours");
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		table.close();
	}

}