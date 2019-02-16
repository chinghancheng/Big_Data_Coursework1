package mapreduce.job1;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class reducer extends Reducer<Text, Text, Text, Text> {
    @Override


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String page = key.toString() + "\t";
        String initial_score = "1.0\t";

        boolean first_link = true;

        for (Text value : values) {
            if(!first_link) initial_score += ",";

            initial_score += value.toString();
            first_link = false;
        }

        context.write(key, new Text(page + initial_score));

    }
}