package CountCat;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CategoryMapper extends Mapper <LongWritable,Text,Text,IntWritable>
{

	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String[] category = value.toString().split("\t");
		
		if(category[0].equals("-")==false)
		context.write(new Text(category[0]), new IntWritable(1));
	}
	
}
