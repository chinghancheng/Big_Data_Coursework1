package mapreduce;

import mapreduce.job1.reducer;
import mapreduce.job1.mapper;
import mapreduce.job1.inputFormat;


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

import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.text.DecimalFormat;
import java.text.NumberFormat;

public class MyPageRank extends Configured implements Tool {

//    private static NumberFormat nf = new DecimalFormat("00");

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new MyPageRank(), args));
    }

    public int run(String[] args) throws Exception {
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("job1(" + args[0] + ")");

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

        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(job1.getJobName() + "_output"));

        return job1.waitForCompletion(true) ? 0 : 1;


    }
}
//        boolean isCompleted = runXmlParsing("wiki/in", "wiki/ranking/iter00");
//        if (!isCompleted) return 1;

//        String lastResultPath = null;
//
//        for (int runs = 0; runs < 5; runs++) {
//            String inPath = "wiki/ranking/iter" + nf.format(runs);
//            lastResultPath = "wiki/ranking/iter" + nf.format(runs + 1);
//
//            isCompleted = runRankCalculation(inPath, lastResultPath);
//
//            if (!isCompleted) return 1;
//        }
//
//        isCompleted = runRankOrdering(lastResultPath, "wiki/result");
//
//        if (!isCompleted) return 1;
//        return 0;
//    }


//    public boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
//
//    }
