import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MapredMongoConfigUtil;
import com.mongodb.hadoop.util.MongoConfigUtil;
import com.mongodb.hadoop.util.MongoTool;

import java.net.UnknownHostException;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.hadoop.io.MongoUpdateWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Iterator;


public class WordCount extends MongoTool {

  public static class TokenizerMapper
       extends Mapper<Object, BSONObject, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, BSONObject value, Context context
                    ) throws IOException, InterruptedException {
      String content = (String) value.get("headline_text");
      StringTokenizer itr = new StringTokenizer(content.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> 
       {
        
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public WordCount() throws UnknownHostException {
    setConf(new Configuration());

    if (MongoTool.isMapRedV1()) {
        MapredMongoConfigUtil.setInputFormat(getConf(), com.mongodb.hadoop.mapred.MongoInputFormat.class);
        MapredMongoConfigUtil.setOutputFormat(getConf(), com.mongodb.hadoop.mapred.MongoOutputFormat.class);
    } else {
        MongoConfigUtil.setInputFormat(getConf(), MongoInputFormat.class);
        MongoConfigUtil.setOutputFormat(getConf(), MongoOutputFormat.class);
    }
    
    MongoConfigUtil.setInputURI(getConf(), "mongodb://localhost:27017/phuonganh.news");
    MongoConfigUtil.setOutputURI(getConf(), "mongodb://localhost:27017/phuonganh.counts");

    MongoConfigUtil.setMapper(getConf(), TokenizerMapper.class);
    MongoConfigUtil.setCombiner(getConf(), IntSumReducer.class);
    MongoConfigUtil.setReducer(getConf(), IntSumReducer.class);
    MongoConfigUtil.setMapperOutputKey(getConf(), Text.class);
    MongoConfigUtil.setMapperOutputValue(getConf(), IntWritable.class);
    MongoConfigUtil.setOutputKey(getConf(), Text.class);
    MongoConfigUtil.setOutputValue(getConf(), IntWritable.class);
}

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new WordCount(), args));


  }
}