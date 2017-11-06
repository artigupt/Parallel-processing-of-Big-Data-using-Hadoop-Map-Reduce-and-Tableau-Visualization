
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class Problem10 {
    public static class TokenizerMapper
    extends Mapper<Object,Text,Text, IntWritable>{
        private final static IntWritable one=new IntWritable(1);
        private Text word=new Text();
        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
            StringTokenizer itr=new StringTokenizer(value.toString(),"\n");
            //String[] line = value.toString().split(",");

          while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String[] line = value.toString().split(",");
         	String[] sub = line[8].split(" ");
		 String result = line[3]+"\t\t"+line[8] + "\t";
       
                try{   if(sub[1].equals("Yoga")){

                        int capacity = Integer.parseInt(line[9]);
			if(capacity>0)
                        context.write(new Text(result), new IntWritable(capacity)); }
} catch(Exception e) {
}
}

                  } 
               }


   public static class IntSumReducer 
   extends Reducer<Text,IntWritable,Text,IntWritable>{
    private IntWritable result=new IntWritable();
    public void reduce(Text key,Iterable<IntWritable> values,
       Context context
       ) throws IOException, InterruptedException{
       Iterator<IntWritable> iterator=values.iterator();
       int sum=iterator.next().get();

       result.set(sum);
       context.write(key,result);
   }
}

public static void main(String[] args) throws Exception{
    Configuration conf=new Configuration();
    Job job=Job.getInstance(conf,"Problem10");
    job.setJarByClass(Problem10.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job,new Path(args[0]));
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    System.exit(job.waitForCompletion(true)?0:1);
}
}
