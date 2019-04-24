import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SON {
    private static List<Long> mapper = new ArrayList<Long>();
    private static List<Long> reducer = new ArrayList<Long>();
    public static class job_one_Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        //input: complete text act duke orsinos palace seacoast olivias house ii street garden iii
        //output: complete 1
        //        text     1
         private static int getbasketslen(String directory){
        try{
            int sum =0;
            Path pt=new Path("hdfs://hadoop-master:9000/user/hadoop/input");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] fileStatus = fs.listStatus(pt);
            for(FileStatus s : fileStatus){
                System.out.println(s.getPath().toString());
                Path txt = new Path(s.getPath().toString());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(txt)));
                String line;
                line=br.readLine();
                while (line != null){
                    sum++;
                    line = br.readLine();
                }
            }
            System.out.println(sum);
            return sum;
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return 0;
    }
        private static int basketlen = getbasketslen("input");
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            Apriori a = new Apriori(value.toString(),20,0.0025/27.0);
            Map<HashSet<String>,Integer> CandidateFrequentTriple = a.GetTrulyTripleFreqDict();
            Iterator<Map.Entry<HashSet<String>, Integer>> it = CandidateFrequentTriple.entrySet().iterator();
            int count=0;
            while (it.hasNext()) {
                if(count==20)break;
                Map.Entry<HashSet<String>, Integer> pair = it.next();
                word.set(pair.getKey().toString());
                context.write(word,new IntWritable(pair.getValue()));
                count++;
            }

        }
    }
    public static class job_one_Reducer
            extends Reducer<Text, IntWritable, Text, NullWritable> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
                      context.write(key,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String separator = " ";
        Job job = Job.getInstance(conf, "Generate frequent 1-itemset");
        Configuration c = job.getConfiguration();
        c.set("mapred.textoutputformat.separator", separator);
        c.set("mapred.output.textoutputformat.separator", separator);
        c.set("mapred.output.key.field.separator", separator);
        c.set("mapred.textoutputformat.separatorText", separator);
        //c.setBoolean("mapred.compress.map.output",true);
        //c.setClass("mapred.map.output.compression.codec",GzipCodec.class, CompressionCodec.class)qq
        job.setNumReduceTasks(1);
        job.setJarByClass(SON.class);
        job.setMapperClass(job_one_Mapper.class);
        job.setReducerClass(job_one_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setMaxInputSplitSize(job,1024*1024*8);
        job.setInputFormatClass(CustomFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("job1_output"));
        if(job.waitForCompletion(true))System.exit(0);
        System.exit(1);
    }
}
