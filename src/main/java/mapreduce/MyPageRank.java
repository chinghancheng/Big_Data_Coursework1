package mapreduce;

import mapreduce.job1.reducer;
import mapreduce.job1.mapper;
import mapreduce.job1.inputFormat;
import mapreduce.job2.secondMapper;
import mapreduce.job2.secondReducer;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import java.lang.Integer;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class MyPageRank extends Configured implements Tool {

    private static NumberFormat numOfResults = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MyPageRank(), args));
    }

    public int run(String[] args) throws Exception {
        String inPath = args[0];

        boolean isCompleted = job1_Func(inPath, "iter00");
        if(!isCompleted) return 1;

        int numOfLoops = Integer.parseInt(args[2]);

        String ResultPath = null;

        for(int i = 0; i < numOfLoops; i++){
            String inputPath = "iter" + numOfResults.format(i);
            ResultPath = "iter" + numOfResults.format(i + 1);

            isCompleted = job2_Func(inputPath, ResultPath);

            if (!isCompleted) return 1;

        }

        return 0;

    }

    public boolean job1_Func(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Job job1 = Job.getInstance(getConf(), "job1");

        job1.setJarByClass(MyPageRank.class);

        // Input / Mapper
        job1.setInputFormatClass(inputFormat.class);
        job1.setMapperClass(mapper.class);
        job1.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setReducerClass(reducer.class);

        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        return job1.waitForCompletion(true);

    }
    public boolean job2_Func(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Job job2 = Job.getInstance(getConf());

        job2.setJarByClass(MyPageRank.class);

        // Input / Mapper
        job2.setMapperClass(secondMapper.class);

        // Output / Reducer

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setReducerClass(secondReducer.class);

        FileInputFormat.setInputPaths(job2, new Path(inputPath));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        return job2.waitForCompletion(true);

    }
//    public boolean job3_Func(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
//
//    }

}
