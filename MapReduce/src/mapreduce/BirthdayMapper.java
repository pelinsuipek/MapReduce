package mapreduce;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;;

public class BirthdayMapper extends Mapper<LongWritable, Text, Text, Text >{
	
	private Text name = new Text();
	private Text birthday = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// read one line and parse
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);		
		
		// create key-value pair
		name.set(tokenizer.nextToken());
		birthday.set(tokenizer.nextToken());
		
		// get configuration to access parameters
		Configuration conf = context.getConfiguration();
		
		// select names and birthdays according to restrictions
		if(isInsideSpecifiedDates(birthday.toString(), conf))
			context.write(name, birthday); // send to reducer
	 }
	
	private boolean isInsideSpecifiedDates(String dateString, Configuration conf){
		
		// set date format
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd.MM.yyyy");			
		
		Date date;
		try {
			date = dateFormat.parse(dateString);
			Date firstDate = dateFormat.parse(conf.get("FirstDate"));
			Date secondDate = dateFormat.parse(conf.get("SecondDate"));
			
			// check if date is between 2 given dates
			if(date.compareTo(firstDate) > 0 && date.compareTo(secondDate) < 0)
				return true;
			else if(date.compareTo(firstDate) == 0 || date.compareTo(secondDate) == 0)
				return true;
			else
				return false;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return false;		
	}

}
