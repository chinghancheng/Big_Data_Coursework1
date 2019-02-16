package mapreduce.job2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class secondReducer extends Reducer<Text, Text, Text, Text> {

    private static final float dampingFactor = 0.85F;

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean isPageExisting = false;
        String[] splitRecord;
        float sumPageRanks = 0;
        String outlinks = "";
        String record;

        // For each otherPage:
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
        for (Text value : values){
            record = value.toString();

            if(record.equals("!")) {
                isPageExisting = true;
                continue;
            }

            if(record.startsWith("|")){
                outlinks = "\t"+record.substring(1);
                continue;
            }

            splitRecord = record.split("\t");

            float pageRank = Float.valueOf(splitRecord[1]);
            int countOutLinks = Integer.valueOf(splitRecord[2]);

            sumPageRanks += (pageRank/countOutLinks);
        }

        if(!isPageExisting) return;
        float newRank = dampingFactor * sumPageRanks + (1-dampingFactor);
        String pageWithTab = page + "\t";

        context.write(page, new Text(pageWithTab + newRank + outlinks));
    }
}