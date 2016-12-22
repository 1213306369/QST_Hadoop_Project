package Round2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class CurrentInquire {
	private static Configuration conf = null;
	private static HBaseAdmin admin = null;
	private static Logger log = Logger.getLogger(CurrentInquire.class);
	private static String hTableName = "show_geyansheng";
	private static String[] families = {"show"};
	
	@SuppressWarnings("deprecation")
	public static void main( String[] args ) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
 
    conf = HBaseConfiguration.create(); 
    conf.set("hbase.zookeeper.property.clientport", "2181");
	conf.set("hbase.zookeeper.quorum", "vm10-0-0-2.ksc.com");
	admin = new HBaseAdmin(conf);
	getResult(args[0]);//根据rowkey查询
	admin.close();//关闭连接
    }
	public static Result getResult( String rowKey)  //根据rowkey查询 
            throws IOException {  
        Get get = new Get(Bytes.toBytes(rowKey));  
        HTable table = new HTable(conf, Bytes.toBytes(hTableName));// 获取表  
        Result result = table.get(get);  
        for (KeyValue kv : result.list()) {  
            System.out.println("列族:" + Bytes.toString(kv.getFamily()));  
            System.out.println("列:" + Bytes.toString(kv.getQualifier()));  
            System.out.println("值:" + Bytes.toString(kv.getValue()));  
            System.out.println("时间戳:" + kv.getTimestamp());  
            System.out.println("-------------------------------------------");  
        }  
        return result;  
    }  
}
