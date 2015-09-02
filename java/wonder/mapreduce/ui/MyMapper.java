package wonder.mapreduce.ui;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper;

import java.io.IOException;

/**
 * Mapper模板。请用真实逻辑替换模板内容
 */
public class MyMapper implements Mapper {
//    private Record word;
//    private Record one;
    
    private Record key;
    private Record value;

    public void setup(TaskContext context) throws IOException { 
        key = context.createMapOutputKeyRecord();
        value = context.createMapOutputValueRecord();
    }

    public void map(long recordNum, Record record, TaskContext context) throws IOException {
    	//根据列名获取数据
    	String uid = record.getString(0);
    	String iid = record.getString(1);
    	Long tp = record.getBigint(2);
    	String ug = record.getString(3);
    	String ic = record.getString(4);
    	String dt = record.getString(5);
    	Long hr = record.getBigint(6);
    	
    	//key输出
    	key.set("uid",uid);
        key.set("iid",iid);
        key.set("ic",ic);
        
        //value输出
        value.set("tp",tp);
        value.set("ug",ug);
        value.set("dt",dt);
        value.set("hr",hr);
        
        context.write(key,value);
    }
    
    public void map_user(long recordNum, Record record, TaskContext context) throws IOException {
    	//根据列名获取数据
    	String uid = record.getString(0);
    	String iid = record.getString(1);
    	Long tp = record.getBigint(2);
    	String ug = record.getString(3);
    	String ic = record.getString(4);
    	String dt = record.getString(5);
    	Long hr = record.getBigint(6);
    	
    	//key输出
    	key.set("uid",uid);
        
        //value输出
    	value.set("iid",iid);
    	value.set("ic",ic);
        value.set("tp",tp);
        value.set("ug",ug);
        value.set("dt",dt);
        value.set("hr",hr);
        
//        System.out.println("Map****************************************************************************************");
//        System.out.println("uid:" + uid);
//        System.out.println("iid:" + iid);
//        System.out.println("ic:" + ic);
//        System.out.println("tp:" + tp);
//        System.out.println("ug:" + ug);
//        System.out.println("dt:" + dt);
//        System.out.println("hr:" + hr);
//        System.out.println("Map****************************************************************************************");
        context.write(key,value);
    }

    public void map_item(long recordNum, Record record, TaskContext context) throws IOException {
    	
    }
    public void cleanup(TaskContext context) throws IOException {

    }
}