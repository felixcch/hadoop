import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SON2 {
    private static List<Long> mapper = new ArrayList<Long>();
    private static List<Long> reducer = new ArrayList<Long>();
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
    public static class job_two_Mapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {
        //input: complete text act duke orsinos palace seacoast olivias house ii street gardenontext) throws IOException{

        private static HashSet<HashSet<String>> readFile(String path){
            HashSet<HashSet<String>> candidatePair = new HashSet<HashSet<String>>();
            try{
                FileSystem fs = FileSystem.get(new Configuration());
                Path pt=new Path("hdfs://hadoop-master:9000/user/hadoop/job1_output");//Location of file in HDFS
                FileStatus[] fileStatus = fs.listStatus(pt);
                for(FileStatus s : fileStatus){
                System.out.println(s.getPath().toString());
                if(!s.getPath().getName().startsWith("part"))continue;
                System.out.println(s.getPath().toString());
                Path txt = new Path(s.getPath().toString());
                BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(txt)));
                String line;
                line=br.readLine();
                while (line != null){
                    String[] items = line.replace("[","").replace("]","").replace(",","").split(" ");
                    HashSet<String> h = new HashSet<String>(Arrays.asList(items));
                    candidatePair.add(h);
                    line=br.readLine();

                }
            }
            }catch(Exception e){
            }
               

            return candidatePair;
        }
        private Boolean issubset(HashSet<String> pair, HashSet<String> items){
            for(String i  :pair){
                if(!items.contains(i))return false;
            }
            return true;
        }
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static HashSet<HashSet<String>> candidatePairList = readFile("part-r-00000");
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            HashSet<String> items = new HashSet<String>(Arrays.asList(line.split(" ")));
            for(HashSet<String> pair : candidatePairList){
                if(issubset(pair,items)){
                    word.set(pair.toString());
                    context.write(word,one);
                }
            }
        }
    }

    public static class job_two_Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        //input: 0,(1,3) 0,(1,4) 0,(2,4) ...
        private Text result = new Text();
        private int basketlen  = getbasketslen("input");
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum=0;
            Iterator<IntWritable> t = values.iterator();
            while(t.hasNext()){
                sum++;
                t.next();
            }
            if((float)(sum)/(float)(basketlen)>0.005){
                context.write(key, new IntWritable(sum));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        String separator = " ";
        Configuration conf = new Configuration();
        Job job2 = Job.getInstance(conf, "Verify truly frequentItemSet");
        Configuration c2 = job2.getConfiguration();
        c2.set("mapred.textoutputformat.separator", separator);
        c2.set("mapred.output.textoutputformat.separator", separator);
        c2.set("mapred.output.key.field.separator", separator);
        c2.set("mapred.textoutputformat.separatorText", separator);
        job2.setNumReduceTasks(20);
        FileInputFormat.setMaxInputSplitSize(job2,1024*1024*8);
        job2.setJarByClass(SON2.class);
        job2.setMapperClass(job_two_Mapper.class);
        job2.setReducerClass(job_two_Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2,new Path(args[0]));
        FileOutputFormat.setOutputPath(job2,new Path("job2_output"));
        if(job2.waitForCompletion(true))
            System.exit(0);

        System.exit(1);
    }
}
