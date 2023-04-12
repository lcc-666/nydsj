package work4;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SearchIndexReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> docsMap = new HashMap<>();
        for (Text docId : values) {
            String id = docId.toString();
            if (docsMap.containsKey(id)) {
                docsMap.put(id, docsMap.get(id) + 1);
            } else {
                docsMap.put(id, 1);
            }
        }

        StringBuilder docs = new StringBuilder();
        for (String docId : docsMap.keySet()) {
            docs.append(docId).append("-->").append(docsMap.get(docId)).append("\t");
        }
        docs.deleteCharAt(docs.length() - 1);

        context.write(key, new Text(docs.toString()));
    }
}
