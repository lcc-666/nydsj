package work4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class SearchIndexMapper extends Mapper<Object, Text, Text, Text> {

    private final Text word = new Text();
    private final Text docId = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();

        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            docId.set(fileName);
            context.write(word, docId);
        }
    }
}
