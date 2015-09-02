package wonder.mapreduce.c;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;    
    private int offsize =  1 ;
    
    private int c_view_count = 0;		//被点击数
    private int c_collect_count =0;		//被收藏数
    private int c_cart_count = 0;		//被加入购物车数
    private int c_buy_count = 0;		//被购买数
    private int c_behavior_count = 0;	//被行为总数
    
    private int c_view_users = 0;		//点击的user数
    private int c_collect_users =0;		//收藏的user数
    private int c_cart_users = 0;		//加入购物车的user数
    private int c_buy_users = 0;		//购买的user数
    private int c_behavior_users = 0;	//行为的user数

    private double c_user_avg_view_count = 0;		//平均每user点击数
    private double c_user_avg_buy_count = 0;		//平均每user购买数
    private double c_user_avg_view_days_count = 0;	//用户平均点击天数
    private double c_user_avg_buy_days_count = 0;	//用户平均购买天数
    
    private double c_collect_ratio = 0;				//收藏率
    private double c_cart_ratio = 0;				//加入购物车率
    private double c_buy_ratio = 0;					//购买率
    private double c_collect_ratio_log_view = 0;	//收藏率*log(view)
    private double c_cart_ratio_log_view = 0;		//加入购物车率*log(view)
    private double c_buy_ratio_log_view = 0;		//购买率*log(view)
    
    private double MC0 ;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
     
    public void init()
    {
    	c_view_count = 0;		//点击数
        c_collect_count =0;		//收藏数
        c_cart_count = 0;		//加入购物车数
        c_buy_count = 0;		//购买数     
        c_behavior_count = 0;	
    	c_view_users = 0;		
    	c_collect_users =0;		
    	c_cart_users = 0;		
    	c_buy_users = 0;		
    	c_behavior_users = 0;	
    	
    	c_user_avg_view_count = 0;	//平均每user点击数
        c_user_avg_buy_count = 0;	//平均每user购买数
        c_user_avg_view_days_count = 0;		//用户平均点击天数
        c_user_avg_buy_days_count = 0;		//用户平均购买天数
        
        c_collect_ratio = 0;			//收藏率
        c_cart_ratio = 0;				//加入购物车率
        c_buy_ratio = 0;					//购买率
        c_collect_ratio_log_view = 0;	//收藏率*log(view)
        c_cart_ratio_log_view = 0;		//加入购物车率*log(view)
        c_buy_ratio_log_view = 0;		//购买率*log(view)
        
        MC0 = 0.00001 ;
        
        set_c_view_user.clear();
        set_c_collect_user.clear();
        set_c_cart_user.clear();
        set_c_buy_user.clear();
        set_c_behavior_user.clear();
    }
    
    private Set set_c_view_user = new HashSet();
    private Set set_c_collect_user = new HashSet();
    private Set set_c_cart_user = new HashSet();
    private Set set_c_buy_user = new HashSet();
    private Set set_c_behavior_user = new HashSet();
    
    public void genFeature(String uid,String iid,int tp,int hr,int dt_m,int dt_d)
    {
    	switch(offsize)
    	{
    	case 0:
    		if (dt_m == 12 && dt_d == 18)
    			return;
    		break;
    	case 1:
    		if (dt_m == 11 && dt_d == 18)
    			return;
    		break;	
    	}
    	switch(tp)
    	{
    	case 1:
    		c_view_count++;
    		set_c_view_user.add(uid);
    		break;
    	case 2:
    		c_collect_count++;
    		set_c_collect_user.add(uid);
    		break;
    	case 3:
    		c_cart_count++;
    		set_c_cart_user.add(uid);
    		break;
    	case 4:
    		c_buy_count++;
    		set_c_buy_user.add(uid);
    		break;
    	}
    	c_behavior_count++;
    	set_c_behavior_user.add(uid);
    }
    
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException 
    {
    	long count = 0;
        init();      
        
        // by lemon 
    	lm_init();
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
 		Date predDay = new Date();
		try {
			predDay = df.parse("2014-12-"+Integer.toString( 18 + offsize ) );
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
      //遍历key的所有value，并累加
        while (values.hasNext()) {
            Record val = values.next();    
            String uid = val.getString("uid");
            String iid = val.getString("iid");
            int tp = new Long(val.getBigint("tp")).intValue();
            String dt = val.getString("dt");
            int hr = new Long(val.getBigint("hr")).intValue();
            String[] parts = dt.split("-");
            
            // by lemon 
            String dt_YMD ="";
            int dt_m,dt_d;
            if (dt.length() <=6 ){
            	dt_YMD = "2014-"+dt ;
            	dt_m = Integer.parseInt(parts[0]);
                dt_d = Integer.parseInt(parts[1]);
            	}
            else {
            	dt_YMD = dt;
            	dt_m = Integer.parseInt(parts[1]);
                dt_d = Integer.parseInt(parts[2]);
            	}

            genFeature(uid, iid, tp, hr, dt_m, dt_d);
        
            // by lemon
            Date date = new Date();
			try {
				date = df.parse(dt_YMD);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		long I = predDay.getTime() - date.getTime() ;
    		long timeDistance = I/(24*60*60*1000);
    		int deteLevel = dateCrossLevel(timeDistance);
            lm_makingFeatures(uid, iid, tp, hr, deteLevel);
        }
        
        int i_f[] = new int[20];
        double d_f[] = new double[10];
        i_f[0] = c_view_count;
        i_f[1] = c_collect_count;
        i_f[2] = c_cart_count;
        i_f[3] = c_buy_count;
        i_f[4] = c_behavior_count;
        i_f[5] = c_view_users = set_c_view_user.size();
        i_f[6] = c_collect_users = set_c_collect_user.size();
        i_f[7] = c_cart_users = set_c_cart_user.size();
        i_f[8] = c_buy_users = set_c_buy_user.size();
        i_f[9] = c_behavior_users = set_c_behavior_user.size();
        d_f[0] = c_collect_ratio = (double)(c_collect_count +MC0) / (c_behavior_count +MC0);
        d_f[1] = c_cart_ratio = (double)(c_cart_count +MC0) / (c_behavior_count +MC0);		
        d_f[2] = c_buy_ratio = (double)(c_buy_count +MC0) / (c_behavior_count +MC0);
        d_f[3] = c_collect_ratio_log_view = c_collect_ratio * Math.log(c_view_count +MC0);
        d_f[4] = c_cart_ratio_log_view = c_cart_ratio * Math.log(c_view_count +MC0);
        d_f[5] = c_buy_ratio_log_view = c_buy_ratio * Math.log(c_view_count +MC0);
        
        d_f[6] = (c_view_count *1.0 ) / (c_view_users + MC0);
        d_f[7] = (c_buy_count *1.0 ) / (c_buy_users + MC0);
        
        
        result.set(0,key.getString("ic"));
       
        int counter = 1 ;
        
        for (int i = 0; i < 10; i++)
        {
        	result.set(i + 1, i_f[i]) ; counter++ ;
        }
        for (int j = 0; j < 8; j++)
        {
        	result.set(j + 11, d_f[j]);counter++ ;
        }

        
        // by lemon 
        ArrayList lm_fList = new ArrayList();
        
        for(int i=0 ; i <5 ; i++ ){
        	for(int j =0 ;j <4;j++){
        		lm_fList.add( c_tp_dayx_y_cnt[i][j] );
        	}
        }
        
        for(Iterator it = lm_fList.iterator();it.hasNext();){
            result.set( counter ,it.next());
            counter ++;
       }
        
        
        context.write(result);
    }
    

    public void cleanup(TaskContext arg0) throws IOException {

    }
    
    
    // by lemon 
    private int[][] c_tp_dayx_y_cnt;
    
    public int dateCrossLevel( long d){
    	if (d ==1 )return 5;
    	else if (d <=2 ) return 4 ;
    	else if (d <=3 ) return 3;
    	else if (d <= 7) return 2 ;
    	else if (d <= 14) return 1 ;
    	return 0;
    }
    
    public void lm_makingFeatures(String uid,String iid,int tp, int hr,int dtLevel){
    	for(int i =0 ; i< dtLevel;i++){
    		c_tp_dayx_y_cnt[4-i][tp-1] +=1;  		
    	}
    	
    }
    
    public void lm_init(){
    	c_tp_dayx_y_cnt = new int[5][4] ; // 1,2,3,7,14 
    }
    
    
    
}
