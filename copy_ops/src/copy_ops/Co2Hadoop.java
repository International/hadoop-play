package copy_ops;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Co2Hadoop {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:8020/user/cloudera/");
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] statuses = fs.listStatus(new Path("."));
    for(FileStatus stat : statuses) {
      if(stat.isDir()) {
        System.out.println("Dir:" + stat.getPath());        
      }
    }
  }
}
