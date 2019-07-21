package weblog;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;

public class ParseMapper extends Mapper<LongWritable,Text,NullWritable,Text>
{
	static String LOG_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[(.+?)\\] \"([^\"]*)\" (\\S+) (\\S+) \"([^\"]*)\" \"([^\"]*)\"";
	static int NUM_FIELDS = 9;
	 Pattern pattern  = null;
	MultipleOutputs <NullWritable, Text> mos = null;

	protected void setup(Context context)
	{
		mos = new MultipleOutputs<NullWritable, Text>(context);
		pattern = Pattern.compile(LOG_PATTERN);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		mos.close();
	}
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
	{
		String fval = value.toString().replaceAll("\t", " ").trim();				
		
		Matcher matcher = pattern.matcher(fval);
		
		if (matcher.matches() && NUM_FIELDS == matcher.groupCount())
		{
			String reqstring = matcher.group(5);													
			String sepreqcategory = separateCategories(reqstring);
			
			StringBuffer valbuf = new StringBuffer();
			valbuf.append(matcher.group(1)).append("\t");					//remoteIP  14.97.118.184
			valbuf.append(matcher.group(2)).append("\t");					//remotelogname
			valbuf.append(matcher.group(3)).append("\t");					//user
			valbuf.append(matcher.group(4)).append("\t");					//time
			valbuf.append(matcher.group(5)).append("\t");					//requeststr
			
			valbuf.append(sepreqcategory).append("\t");				//cat1 cat2 cat3 cat4 page param
			
			valbuf.append(matcher.group(6)).append("\t");					//statuscode
			valbuf.append(matcher.group(7)).append("\t");					//bytestring
			valbuf.append(matcher.group(8)).append("\t");					//user-agent
			valbuf.append(matcher.group(9));													
			
//			context.write(NullWritable.get(), new Text (valbuf.toString()));
			mos.write("SuccessfullyParsed", NullWritable.get(), new Text (valbuf.toString()));
		}
		else
		{
			mos.write("ErrorRecords", NullWritable.get(), value);
		}
	}
		
	private String separateCategories(String reqstring)
	{
		String reqstringTokens [] = reqstring.split(" ");
		
		String sepreqcategory = null;
		if (reqstringTokens.length == 3)
			sepreqcategory = getProcessedRequest(reqstringTokens[1]);
		else
			sepreqcategory = getProcessedDefaultRequest();
		return sepreqcategory;
	}
		
	String getProcessedRequest(String request)						//	/a/b/c/d?param
	{
		StringBuffer sepreqcategoryBuffer = new StringBuffer();
		
		String requestParamTokens [] = request.split("\\?");						
		String ParamString = "-";													
		boolean paramFlag = false;
		if (requestParamTokens.length == 2)											//? one time
		{
			paramFlag = true;
			ParamString = requestParamTokens[1];
		}
		else if (requestParamTokens.length > 2)										
		{
			paramFlag = true;
			StringBuffer paramStrBuff = new StringBuffer();
			for (int cnt = 1; cnt < requestParamTokens.length; cnt++)
			{
				paramStrBuff.append(requestParamTokens[cnt]);
				if (cnt < requestParamTokens.length - 1)
					paramStrBuff.append("?");
			}
			ParamString = paramStrBuff.toString();
		}
		
		String reqtokens [] = null;
		if (paramFlag)
			reqtokens = requestParamTokens[0].split("/");			//Request = /a/b/c	(case for /a/b/c?param)
		else
			reqtokens = request.split("/");							//Request = /a/b/c	(case for /a/b/c)
		
		
		int reqLen = reqtokens.length;
		if (reqLen == 0)													// for /
		{
			sepreqcategoryBuffer.append("/").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-");
		}
//		else if (reqLen == 1)										
//		{
//			sepreqcategoryBuffer.append("-").append("\t");
//			sepreqcategoryBuffer.append("-").append("\t");
//			sepreqcategoryBuffer.append("-").append("\t");
//			sepreqcategoryBuffer.append("-").append("\t");
//			sepreqcategoryBuffer.append("-");
//		}
		else if (reqLen == 2)										// for /abc.html
		{
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append(reqtokens[1]);
		}
		else if (reqLen == 3)										//for /a/abc.html
		{
			sepreqcategoryBuffer.append(reqtokens[1]).append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append(reqtokens[2]);
		}
		else if (reqLen == 4)										// for /a/b/abc.html
		{
			sepreqcategoryBuffer.append(reqtokens[1]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[2]).append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append(reqtokens[3]);
		}
		else if (reqLen == 5)
		{
			sepreqcategoryBuffer.append(reqtokens[1]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[2]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[3]).append("\t");
			sepreqcategoryBuffer.append("-").append("\t");
			sepreqcategoryBuffer.append(reqtokens[4]);
		}
		else if (reqLen == 6)
		{
			sepreqcategoryBuffer.append(reqtokens[1]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[2]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[3]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[4]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[5]);
		}
		else if (reqLen > 6)
		{
			sepreqcategoryBuffer.append(reqtokens[1]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[2]).append("\t");
			sepreqcategoryBuffer.append(reqtokens[3]).append("\t");
			StringBuffer reqtokensBuffer = new StringBuffer();
			for (int cnt = 4; cnt < reqLen - 1; cnt++)
			{
				reqtokensBuffer.append(reqtokens[cnt]).append("/");
			}
			sepreqcategoryBuffer.append(reqtokensBuffer).append("\t");
			sepreqcategoryBuffer.append(reqtokens[reqLen - 1]);
		}
		
		
//		
		sepreqcategoryBuffer.append("\t").append(ParamString);
		return sepreqcategoryBuffer.toString();
	}
	
	
	String getProcessedDefaultRequest()
	{
		StringBuffer sepreqcategoryBuffer = new StringBuffer();
		
		sepreqcategoryBuffer.append("-").append("\t");				//cat1
		sepreqcategoryBuffer.append("-").append("\t");				//cat2
		sepreqcategoryBuffer.append("-").append("\t");				//cat3
		sepreqcategoryBuffer.append("-").append("\t");				//cat4
		sepreqcategoryBuffer.append("-").append("\t");				//page
		
		sepreqcategoryBuffer.append("-");										//param

		return sepreqcategoryBuffer.toString();
	}
	
}	

