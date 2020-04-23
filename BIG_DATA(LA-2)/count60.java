package my.mapred.pac;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
	
public class count60 {

//MAPPER CODE	
	   
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
private final static IntWritable one = new IntWritable(1);
//private Text word = new Text();
Text myText = new Text();
public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	String myString = value.toString(); 
	String[] csvArray = myString.split(",");
	myText.set("mykey");
	IntWritable subject1 = new IntWritable(Integer.parseInt(csvArray[2]));
	IntWritable subject2 = new IntWritable(Integer.parseInt(csvArray[3]));
	IntWritable subject3 = new IntWritable(Integer.parseInt(csvArray[4]));
	if(subject1.get()>39 && subject2.get()>39 && subject3.get()>39)
	{
		output.collect(myText,one);
	}
	
	}  
}

//REDUCER CODE	
public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { //{little: {1,1}} 
	int count = 0 ; 
	Text key = t_key ;
	while(values.hasNext()) {
		IntWritable value = values.next(); 
		count = count+value.get(); 
	}
	output.collect(key, new IntWritable(count));
	}
}
	
//DRIVER CODE
public static void main(String[] args) throws Exception {
	JobConf conf = new JobConf(count60.class);
	conf.setJobName("first");
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class); // hadoop jar jarname classpath inputfolder outputfolder
	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	JobClient.runJob(conf);   
}
}