//public class WordCount extends Configured implements Tool{
//    static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable,
//            Text, Text, IntWritable>{
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        public void map(LongWritable key, Text value, Context context) throws
//                IOException, InterruptedException {
//            String line = value.toString();
//            StringTokenizer tokenizer = new StringTokenizer(line);
//            while (tokenizer.hasMoreTokens()){
//                word.set(tokenizer.nextToken());
//                context.write(word, one);
//            }
//        }
//    }
//
//    public static  class  Reduce extends Reducer<Text, IntWritable, Text,
//            IntWritable> {
//        public void reduce(Text key, Iterable<IntWritable> values, Context context)
//            throws IOException, InterrupedException {
//            int sum = 0;
//            for (IntWritable value: values)
//                sum += value.get();
//            context.write(key, new IntWritable(sum));
//        }
//    }
//}