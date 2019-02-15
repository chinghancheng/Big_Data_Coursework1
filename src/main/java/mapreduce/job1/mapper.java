public class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text target_article_title = new Text();
    private Text source_article_titles = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {




    }

    private String[] parseTitleAndText(Text value) throws CharacterCodingException {
        String[] titleAndList = new String[2];

        int start = value.find("REVISION");
        for(int i = 0; i < 3; i++){
            int start = value.find(" ", start);
        }
        int end = value.find(" ", start);
        start += 1;

        titleAndList[0] = Text.decode(value.getBytes(), start, end-start);

        start = value.find("MAIN");

        for(int i = 0; i < 3; i++){

            start = value.find(" ", start);
            end = value.find(" ", start);

        }
        end = value.find(" ", start);
        start += 1;

    }
}