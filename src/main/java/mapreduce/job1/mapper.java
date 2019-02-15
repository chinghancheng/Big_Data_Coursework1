package mapreduce.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;

public class mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String record = value.toString();
        String[] title_name= record.split("\r");

        String[] revision_content = title_name[0].split(" ");
        String[] main_content = title_name[2].split(" ");

        Text target_article_title = new Text(revision_content[3]);

        int main_size = main_content.length;
        for(int i = 0; i < main_size; i++){
            if(main_content[i] == target_article_title.toString())
                continue;
            context.write(target_article_title, new Text(main_content[i]));
        }

//        String[] titles = parseTitle(value);

    }

//    private String[] parseTitle(Text value) throws CharacterCodingException {
//        String[] titles = new String[1];
//
//
//        int start = value.find("REVISION");
//        for(int i = 0; i < 3; i++){
//            int start = value.find(" ", start);
//        }
//        int end = value.find(" ", start);
//        start += 1;
//
//        titles[0] = Text.decode(value.getBytes(), start, end-start);
//        Text target_article_title = new Text(titles[0]);
//
//        start = value.find("MAIN");
//        mark = value.find("TALK");
//
//        for(){
//
//            start = value.find(" ", start);
//            end = value.find(" ", start);
//            if()
//            start += 1;
//
//            outlink = Text.decode(value.getBytes(), start, end-start);
//            context.write(target_article_title, new Text(outlink));
//
//        }
//
//    }
}