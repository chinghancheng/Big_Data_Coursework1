package mapreduce.job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class secondMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int sourcePageTabIndex = value.find("\t");
        int sourceRankTabIndex = value.find("\t", sourcePageTabIndex+8);

        String sourcePage = Text.decode(value.getBytes(), 0, sourcePageTabIndex);
        String sourcePageWithRank = Text.decode(value.getBytes(), 0, sourceRankTabIndex+8);

        // Mark page as an Existing page (ignore red wiki-links)
        context.write(new Text(sourcePage), new Text("!"));

        // Skip pages with no links.???????????????????????????????????????????????
        if(sourceRankTabIndex == -1) return;
        //?????????????????????????????????????????????????????????????????

        String outlinksOfSource = Text.decode(value.getBytes(), sourceRankTabIndex+8, value.getLength()-(sourceRankTabIndex+8));
        String[] outlinkPages = outlinksOfSource.split(",");
        int numOfLinks = outlinkPages.length;

        for (String outlinkPage : outlinkPages){
            Text output = new Text(sourcePageWithRank + numOfLinks);
            context.write(new Text(outlinkPage), output);
        }

        // Put the original links of the page for the reduce output
        context.write(new Text(sourcePage), new Text("|" + outlinksOfSource));
    }
}