public class inputFormat extends FileInputFormat<LongWritable, Text> {
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new RecordReader();
    }
}