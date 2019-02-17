package mapreduce.job1;

import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
//    private String[] outlinkSet;
    @Override

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String titles ;
        String outlink;
        String timestampOfRevision;
        String date;
        boolean isRepeted = false;
        List outlinkList = new ArrayList();

        Configuration conf = context.getConfiguration();
        String userISO_8601 = conf.get("ISO_8601");


        int start = value.find("REVISION");
        for (int i = 0; i < 3; i++) {
            start = value.find(" ", start+1);
        }
        int end = value.find(" ", start+1);
        start += 1;

        titles = Text.decode(value.getBytes(), start, end - start);
        Text target_article_title = new Text(titles);

        start = value.find(" ", start+1);
        end = value.find(" ", start+1);

        timestampOfRevision = Text.decode(value.getBytes(), start, end - start);

        long userTime = 0;
        long recordTime = 0;

        try {
            userTime = utils.ISO8601.toTimeMS(userISO_8601);
            recordTime = utils.ISO8601.toTimeMS(timestampOfRevision);
        } catch (ParseException e) {
                e.printStackTrace();
        }
//        long a = utils.ISO8601.toTimeMS("2008-01-03T00:00:00Z");

        if(recordTime > userTime)
            return;

        start = value.find("MAIN");
        int mark = value.find("TALK");
        int run = 0;

        while (end + 1 < mark) {
            start = value.find(" ", start+1);
            end = value.find(" ", start+1);
            if (end > mark)
                end = value.find("\n", start+1);

            start += 1;

            outlink = Text.decode(value.getBytes(), start, end - start);

            //check self-loop
            if(outlink == target_article_title.toString())
                continue;

            //check same outlink
            if(run > 0){
//                for(int i = 0;i < outlinkSet.length; i++ ){
//                    if(outlinkSet[i] == outlink)
//                        isRepeted = true;
//                }
                for(int i = 0;i < outlinkList.size(); i++ ){
                    if(outlinkList.contains(outlink))
                        isRepeted = true;
                }
            }

            if(!isRepeted){
                outlinkList.add(outlink);
//                outlinkSet[run] = outlink;
                context.write(target_article_title, new Text(outlink));
                run++;
            }
            isRepeted = false;

        }

    }

//    public static long toTimeMS(final String iso8601string) throws ParseException {
//        String s = iso8601string.replace("Z", "+00:00");
//        Date date;
//
//        try {
//            s = s.substring(0, 22) + s.substring(23); // to get rid of the ":"
//            date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").parse(s);
//
//        } catch (IndexOutOfBoundsException e) {
//            throw new ParseException("Invalid length", 0);
//        }
//        return date.getTime();
//    }
}

//    String record = value.toString();
//    String[] title_name= record.split("\r");
//
//    String[] revision_content = title_name[0].split(" ");
//    String[] main_content = title_name[2].split(" ");
//
//    Text target_article_title = new Text(revision_content[3]);
//
//    int main_size = main_content.length;
//        for(int i = 0; i < main_size; i++){
//        if(main_content[i] == target_article_title.toString())
//        continue;
//        context.write(target_article_title, new Text(main_content[i]));
//        }
//
////        String[] titles = parseTitle(value);