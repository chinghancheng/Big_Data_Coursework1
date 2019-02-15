//package mapreduce;
//
//import java.io.IOException;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//public class MyLoopingMapReduceJob extends Configured implements Tool {
//
////	static class MyComputer throws Exception {
////
////	}
//
//	// Your mapper class; remember to set the input and output key/value class appropriately in the <...> part below.
//	static class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
//		@Override
//		protected void setup(Context context) throws IOException, InterruptedException {
//			super.setup(context);
//			// ...
//		}
//
//		// The main map() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
//		@Override
//		protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
//
////			while(value.hasMoreRevision_record){
////				i=0;
////				thisRecord = nextRecord;
////				while(outlinkListOfMain){
////					targetArticle= thisRecord.main.list[i++];
////					sourceArticle = thisRecord.revision["article_title"];
////					context.wirte(targetArticle,sourceArticle);
////				}
////			}
//		}
//
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ...
//			super.cleanup(context);
//		}
//	}
//
//	// Your reducer class; remember to set the input and output key/value class appropriately in the <...> part below.
//	static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//		@Override
//
////		private list listOfSourceArticle = new list();
//
//		protected void setup(Context context) throws IOException, InterruptedException {
//			super.setup(context);
//			// ...
//		}
//
//		// The main reduce() function; the input key/value classes must match the first two above, and the key/value classes in your emit() statement must match the latter two above.
//		// Make sure that the output key/value classes also match those set in your job's configuration (see below).
//		@Override
//		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
////			for (IntWritable value: values){
////				listOfSourceArticle.append(value);
////			}
////			context.write(key, listOfSourceArticle);
//		}
////
//		@Override
//		protected void cleanup(Context context) throws IOException, InterruptedException {
//			// ...
//			super.cleanup(context);
//		}
//	}
//
//	// Your main Driver method. Note: everything in this method runs locally at the client.
//	public int run(String[] args) throws Exception {
//		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
////		Job job = Job.getInstance(getConf());
////		Job job = Job.getInstance(getConf(), "MyLoopingMapReduceJob");
////
////
////		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
////		job.setJarByClass(MyLoopingMapReduceJob.class);
////
////		// 2. Set mapper and reducer classes
////		job.setMapperClass(MyMapper.class);
////		job.setCombinerClass(MyReducer.class);
////		job.setReducerClass(MyReducer.class);
////
////		// 3. Set final output key and value classes
////		job.setOutputKeyClass(Text.class);     ///???????????
////		job.setOutputValueClass(IntWritable.class);
////
////		// 4. Get #loops from input (args[])
////		int numLoops = new IntWritable(args[2]); // Change this!         ????????????????????????????????????????????
////		String date = new Text(args[3]);
////
////		boolean succeeded = false;
////		for (int i = 0; i < numLoops; i++) {
////			// 5. Set input and output format, mapper output key and value classes, and final output key and value classes
////			//    As this will be a looping job, make sure that you use the output directory of one job as the input directory of the next!
////			job.setInputFormatClass(TextInputFormat.class);     ////////????????????????????????????????????
////			FileInputFormat.setInputPaths(job, new Path(args[0]));
////			job.setOutputKeyClass(Text.class);
////			job.setOutputValueClass(IntWritable.class);
////			job.setOutputFormatClass(TextOutputFormat.class);
////			FileOutputFormat.setOutputPath(job, new Path(args[1]));
////
////			// 6. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
////			// ...?????????????????????????????????????????????
////
////			// 7. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
////			succeeded = job.waitForCompletion(true);
////
////			if (!succeeded) {
////				// 8. The program encountered an error before completing the loop; report it and/or take appropriate action
////				println("Loop Failure!");
////				break;
////			}
////		}
////		return (succeeded ? 0 : 1);
//		return 0;
//	}
//
//	public static void main(String[] args) throws Exception {
//		System.exit(ToolRunner.run(new Configuration(), new MyLoopingMapReduceJob(), args));
//	}
//}
