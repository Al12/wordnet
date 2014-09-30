import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class WordNetMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {		
		String paragraph = value.toString();
		String[] sentences = paragraph.split("\\.");
		for ( String sentence : sentences ) {
			String[] words = sentence.replaceAll("[^\\w\\s]", "").toLowerCase().split("\\s");			
			Text keyText;
			for ( String word : words ) {				
				if ( !word.isEmpty()) {
					keyText = new Text(word);
					for (String neighbour : words ) {
						if ( !word.equals(neighbour) && !neighbour.isEmpty()) {
							context.write(keyText, new Text(neighbour));
						}
					}
				}
			}
		}
	}
}