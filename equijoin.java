 import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hdfs.util.EnumCounters.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;

class Mapclass extends Mapper<LongWritable, Text, Text, Text> {

	  @Override
	  public void map(LongWritable key, Text values, Context con)
	      throws IOException, InterruptedException {
		  
	  String a = values.toString();	  
          String[] b = a.split(",");
	  con.write(new Text(b[1]), values);
	  }
}
class Reduceclass extends Reducer<Text, Text, Text, Text> {

	  @Override
	  public void reduce(Text key, Iterable<Text> val, Context ty)
		      throws IOException, InterruptedException {
			  Iterator keys = val.iterator();
                          List<String> values = new ArrayList();			  
			  while (keys.hasNext())
                          {      String temp = keys.next().toString();
				 values.add(temp);
			  }		  
			  for (int i = 0; i<values.size(); i++){
				  for (int j = i+1; j<values.size();j++){
					  String st1 = values.get(i);
					  String st2 = values.get(j);
                                          String[] words1 = st1.split(",");
                                          String[] words2 = st2.split(",");
                                          if(words1[0].equals(words2[0]))
                                                continue; 
                                          else if (st1.equals(st2))
                                                continue;
					  else 
					        ty.write(null, new Text(st2 + ", "+ st1));
				  }
			  }
		  }
}
public class equijoin {

  public static void main(String[] args) throws Exception {     
    Job j = new Job();
    j.setJobName("mapreduce");  
    j.setJarByClass(equijoin.class);
    j.setMapperClass(Mapclass.class);
    j.setReducerClass(Reduceclass.class);
    j.setOutputKeyClass(Text.class);
    j.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(j, new Path(args[0]));
    FileOutputFormat.setOutputPath(j, new Path(args[1])); 
    System.exit(j.waitForCompletion(true) ? 0 : 1);
  }
}

 
