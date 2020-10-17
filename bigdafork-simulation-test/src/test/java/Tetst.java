import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionImpl;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.spark.SparkContext;
import org.apache.thrift.transport.TSocket;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;

public class Tetst {

    public static void main(String[] args) throws Exception {
        String a = "d,a,a,";
        File file = new File("D:/txt.txt");
        FileOutputStream truncateStream = new FileOutputStream(file, true);
        long iniPos = file.length();
        


    }
}
