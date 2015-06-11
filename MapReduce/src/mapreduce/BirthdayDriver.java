package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BirthdayDriver extends Configured implements Tool {
	
	public static void main(String[] args) throws Exception {
		int value = ToolRunner.run(new Configuration(), new BirthdayDriver(), args);
		System.exit(value); 

	}

	@Override
	public int run(String[] args) throws Exception {
		
		//create job
		Configuration conf = new Configuration();
		// set arguments as parameters to mappers
		conf.set("FirstDate", args[2]);
		conf.set("SecondDate", args[3]);
		Job birthdayjob = Job.getInstance(conf);
		birthdayjob.setJarByClass(BirthdayDriver.class);
				
		//setup mapreduce job
		birthdayjob.setMapperClass(BirthdayMapper.class);
		birthdayjob.setReducerClass(BirthdayReducer.class);
				
		//specify key-value
		birthdayjob.setOutputKeyClass(Text.class);
		birthdayjob.setOutputValueClass(Text.class);
				
		//input
		FileInputFormat.setInputPaths(birthdayjob, new Path(args[0]));
		birthdayjob.setInputFormatClass(TextInputFormat.class);
				
		//output
		FileOutputFormat.setOutputPath(birthdayjob, new Path(args[1]));
		birthdayjob.setOutputFormatClass(TextOutputFormat.class);
				
		//execute job
		birthdayjob.submit();
		return 0;
	}	

}
