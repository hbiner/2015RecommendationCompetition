package wonder.mapreduce.u;

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
    
    private int offsize = 0;
    //（1-4）
    private int u_day14_view_items = 0;		//点击的item数
    private int u_day14_collect_items =0;	//收藏的item数
    private int u_day14_cart_items = 0;		//加入购物车的item数
    private int u_day14_buy_items = 0;		//购买的item数
    
    //（5-8）
    private double u_day14_avg_view_days_count = 0;		//平均每天点击数
    private double u_day14_avg_collect_days_count = 0;	//平均每天收藏数
    private double u_day14_avg_cart_days_count = 0;		//平均每天加入购物车数
    private double u_day14_avg_buy_days_count = 0;		//平均每天购买次数
    
    //（9-13）
    private double u_day14_cart_ratio = 0;			//加入购物车率
    private double u_day14_cart_ratio_log_view = 0;	//加入购物车率*log(view)
    private double day14_buy_ratio = 0;				//购买率
    private double day14_buy_ratio_log_view = 0;	//购买率*log(view)
    private double day14_collect_ratio = 0;			//收藏率
    
    //(14-23)
    private double u_day13_cart2view_items = 0;		//加入购物车item数/点击item数
    private double u_day13_cart2buy_items = 0;		//加入购物车item数/购买item数
    private double u_day13_cart2view_days = 0;
    private double u_day13_cart2buy_days = 0;
    private double u_day13_cart2view_ratio = 0;
    private double u_day13_cart2buy_ratio = 0;
    private double u_day13_cart2view_ratio_log_cart = 0;
    private double u_day13_cart2buy_ratio_log_cart = 0;
    private double u_day13_collect2view_ratio = 0;
    private double u_day13_collect2buy_ratio = 0;
    //24-35
    private int u_day1_view_items = 0;
    private int u_day1_collect_items = 0;
    private int u_day1_cart_items = 0;
    private int u_day1_buy_items = 0;
    private int u_day3_view_items = 0;
    private int u_day3_collect_items = 0;
    private int u_day3_cart_items = 0;
    private int u_day3_buy_items = 0;
    private int u_day7_view_items = 0;
    private int u_day7_collect_items = 0;
    private int u_day7_cart_items = 0;
    private int u_day7_buy_items = 0;
    //36-43
    private int u_day1_bahavior_cross_hours;
    private int u_day3_bahavior_cross_hours;
    private int u_day7_bahavior_cross_hours;
    private int u_day14_bahavior_cross_hours;
    private int u_day1_behavior_cross_days;
    private int u_day3_behavior_cross_days;
    private int u_day7_behavior_cross_days;
    private int u_day14_behavior_cross_days;    
    //44-59
    private int u_day14_view_count;
    private int u_day14_collect_count;
    private int u_day14_cart_count;
    private int u_day14_buy_count;
    private int u_day1_view_count;
    private int u_day1_collect_count;
    private int u_day1_cart_count;
    private int u_day1_buy_count;
    private int u_day3_view_count;
    private int u_day3_collect_count;
    private int u_day3_cart_count;
    private int u_day3_buy_count;
    private int u_day7_view_count;
    private int u_day7_collect_count;
    private int u_day7_cart_count;
    private int u_day7_buy_count;
    //60-67
    private double u_day7_avg_view_days_count;
    private double u_day7_avg_collect_days_count;
    private double u_day7_avg_cart_days_count;
    private double u_day7_avg_buy_days_count;
    private double u_day3_avg_view_days_count;
    private double u_day3_avg_collect_days_count;
    private double u_day3_avg_cart_days_count;
    private double u_day3_avg_buy_days_count;
    //68-102
    private double u_day14_cart_ratio_i;
    private double u_day14_cart_ratio_log_view_i;
    private double u_day14_buy_ratio_i;
    private double u_day14_buy_ratio_log_view_i;
    private double u_day14_collect_ratio_i;
    private double u_day7_cart_ratio_i;
    private double u_day7_cart_ratio_log_view_i;
    private double u_day7_buy_ratio_i;
    private double u_day7_buy_ratio_log_view_i;
    private double u_day7_collect_ratio_i;
    private double u_day7_cart_ratio_a;
    private double u_day7_cart_ratio_log_view_a;
    private double u_day7_buy_ratio_a;
    private double u_day7_buy_ratio_log_view_a;
    private double u_day7_collect_ratio_a;   
    private double u_day3_cart_ratio_i;
    private double u_day3_cart_ratio_log_view_i;
    private double u_day3_buy_ratio_i;
    private double u_day3_buy_ratio_log_view_i;
    private double u_day3_collect_ratio_i;
    private double u_day3_cart_ratio_a;
    private double u_day3_cart_ratio_log_view_a;
    private double u_day3_buy_ratio_a;
    private double u_day3_buy_ratio_log_view_a;
    private double u_day3_collect_ratio_a;
    private double u_day1_cart_ratio_i;
    private double u_day1_cart_ratio_log_view_i;
    private double u_day1_buy_ratio_i;
    private double u_day1_buy_ratio_log_view_i;
    private double u_day1_collect_ratio_i;
    private double u_day1_cart_ratio_a;
    private double u_day1_cart_ratio_log_view_a;
    private double u_day1_buy_ratio_a;
    private double u_day1_buy_ratio_log_view_a;
    private double u_day1_collect_ratio_a;
    //103-114
    private double u_day1_7_to_1_14_view_count_ratio;
    private double u_day1_3_to_1_7_view_count_ratio;
    private double u_day1_to_1_3_view_count_ratio;
    private double u_day1_7_to_1_14_collect_count_ratio;
    private double u_day1_3_to_1_7_collect_count_ratio;
    private double u_day1_to_1_3_collect_count_ratio;
    private double u_day1_7_to_1_14_cart_count_ratio;
    private double u_day1_3_to_1_7_cart_count_ratio;
    private double u_day1_to_1_3_cart_count_ratio;
    private double u_day1_7_to_1_14_buy_count_ratio;
    private double u_day1_3_to_1_7_buy_count_ratio;
    private double u_day1_to_1_3_buy_count_ratio;
    //115-126
    private double u_day1_7_to_1_14_view_items_ratio;
    private double u_day1_3_to_1_7_view_items_ratio;
    private double u_day1_to_1_3_view_items_ratio;    
    private double u_day1_7_to_1_14_collect_items_ratio;
    private double u_day1_3_to_1_7_collect_items_ratio;
    private double u_day1_to_1_3_collect_items_ratio;    
    private double u_day1_7_to_1_14_cart_items_ratio;
    private double u_day1_3_to_1_7_cart_items_ratio;
    private double u_day1_to_1_3_cart_items_ratio;    
    private double u_day1_7_to_1_14_buy_items_ratio;
    private double u_day1_3_to_1_7_buy_items_ratio;
    private double u_day1_to_1_3_buy_items_ratio;
    //127-132
    private double u_day1_7_to_1_14_behavior_cross_days_ratio;
    private double u_day1_3_to_1_7_behavior_cross_days_ratio;
    private double u_day1_to_1_3_behavior_cross_days_ratio;
    private double u_day1_7_to_1_14_behavior_cross_hours_ratio;
    private double u_day1_3_to_1_7_behavior_cross_hours_ratio;
    private double u_day1_to_1_3_behavior_cross_hours_ratio;
    //133-152
    private double u_day6_cart2view_items;
    private double u_day6_cart2buy_items;
    private double u_day6_cart2view_days;
    private double u_day6_cart2buy_days;
    private double u_day6_cart2view_ratio;
    private double u_day6_cart2buy_ratio;
    private double u_day6_cart2view_ratio_log_cart;
    private double u_day6_cart2buy_ratio_log_cart;
    private double u_day6_collect2view_ratio;
    private double u_day6_collect2buy_ratio;
    private double u_day2_cart2view_items;
    private double u_day2_cart2buy_items;
    private double u_day2_cart2view_days;
    private double u_day2_cart2buy_days;
    private double u_day2_cart2view_ratio;
    private double u_day2_cart2buy_ratio;
    private double u_day2_cart2view_ratio_log_cart;
    private double u_day2_cart2buy_ratio_log_cart;
    private double u_day2_collect2view_ratio;
    private double u_day2_collect2buy_ratio;
    
    private double MC0 ;

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
    
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
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
            String iid = val.getString("iid");
            String ic = val.getString("ic");
            int tp = new Long(val.getBigint("tp")).intValue();
            String ug = val.getString("ug");
            int hr = new Long(val.getBigint("hr")).intValue();
            String dt = val.getString("dt");
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
            
            f_1(iid, ic, tp, ug, hr,dt_m, dt_d); 
            f_2(iid, ic, tp, ug, hr,dt_m, dt_d);
            
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
    		lm_makingFeatures(iid,ic,tp, ug,(int)timeDistance, hr);
        }
        int i_f[] = new int[150];
        double d_f[] = new double[150]; 
        //1-13
        i_f[0] = u_day14_view_items = set_d14_view_items.size();
        i_f[1] = u_day14_collect_items = set_d14_collect_items.size();
        i_f[2] = u_day14_cart_items = set_d14_cart_items.size();
        i_f[3] = u_day14_buy_items = set_d14_buy_items.size();
        d_f[0] = u_day14_avg_view_days_count = (double)(d14_sum_act[0] / 14.0);
        d_f[1] = u_day14_avg_collect_days_count = (double)(d14_sum_act[1] / 14.0);
        d_f[2] = u_day14_avg_cart_days_count = (double)(d14_sum_act[2] / 14.0);
        d_f[3] = u_day14_avg_buy_days_count = (double)(d14_sum_act[3] / 14.0);
        d_f[4] = u_day14_cart_ratio = ((double)(d14_sum_act[2] ) / ( d14_sum_act[0] + d14_sum_act[1] + d14_sum_act[2] + d14_sum_act[3] + MC0));
        d_f[5] = u_day14_cart_ratio_log_view = u_day14_cart_ratio * Math.log(d14_sum_act[0] + MC0 +1);
        d_f[6] = day14_buy_ratio = (double)((double)(d14_sum_act[3] ) / (d14_sum_act[0] + d14_sum_act[1] + d14_sum_act[2] + d14_sum_act[3] + MC0));
        d_f[7] = day14_buy_ratio_log_view = day14_buy_ratio * Math.log(d14_sum_act[0] + MC0 + 1);
        d_f[8] = day14_collect_ratio = (double)((double)(d14_sum_act[1] ) / (d14_sum_act[0] + d14_sum_act[1] + d14_sum_act[2] + d14_sum_act[3] + MC0));
        //14-23
        d_f[9] = u_day13_cart2view_items = (double)(d13_sum_act[2] ) / ( d13_sum_act[2] + d13_sum_act[0] + MC0);		
        d_f[10] = u_day13_cart2buy_items = (double)(d13_sum_act[2] ) / ( d13_sum_act[2] + d13_sum_act[3] + MC0);		
        d_f[11] = u_day13_cart2view_days = (double)(set_d13_cart_days.size() ) / ( set_d13_cart_days.size()+set_d13_view_days.size() + MC0);
//        d_f[11] = u_day13_cart2view_days = (double)(set_d13_cart_days.size() ) / (set_d13_cart_days.size()+set_d13_view_days.size() + MC0);
        d_f[12] = u_day13_cart2buy_days = (double)(set_d13_cart_days.size() ) / (set_d13_cart_days.size()+set_d13_buy_days.size() + MC0);
        d_f[13] = u_day13_cart2view_ratio = (double)(d13_sum_act[2] ) / (d13_sum_act[2]+d13_sum_act[0] + MC0);
        d_f[14] = u_day13_cart2buy_ratio = (double)(d13_sum_act[2] ) / (d13_sum_act[2] + d13_sum_act[3] + MC0);
//        d_f[14] = u_day13_cart2buy_ratio = (double)(d13_sum_act[2] + MC0) / (d13_sum_act[3] + MC0);
        d_f[15] = u_day13_cart2view_ratio_log_cart = u_day13_cart2view_ratio * Math.log(d13_sum_act[2] + MC0 + 1);
        d_f[16] = u_day13_cart2buy_ratio_log_cart = u_day13_cart2buy_ratio * Math.log(d13_sum_act[2] + MC0 + 1);
        d_f[17] = u_day13_collect2view_ratio = (double)(d13_sum_act[1] ) / (d13_sum_act[1] + d13_sum_act[0] + MC0);
//        d_f[17] = u_day13_collect2view_ratio = (double)(d13_sum_act[1] + MC0) / (d13_sum_act[0] + MC0);
        d_f[18] = u_day13_collect2buy_ratio = (double)(d13_sum_act[1] ) / (d13_sum_act[3]+d13_sum_act[1]  + MC0);
        //24-35
        i_f[4] = u_day1_view_items = set_day1_view_items.size();
        i_f[5] = u_day1_collect_items = set_day1_collect_items.size();
        i_f[6] = u_day1_cart_items = set_day1_cart_items.size();
        i_f[7] = u_day1_buy_items = set_day1_buy_items.size();
        i_f[8] = u_day3_view_items = set_day3_view_items.size();
        i_f[9] = u_day3_collect_items = set_day3_collect_items.size();
        i_f[10] = u_day3_cart_items = set_day3_cart_items.size();
        i_f[11] = u_day3_buy_items = set_day3_buy_items.size();
        i_f[12] = u_day7_view_items = set_day7_view_items.size();
        i_f[13] = u_day7_collect_items = set_day7_collect_items.size();
        i_f[14] = u_day7_cart_items = set_day7_cart_items.size();
        i_f[15] = u_day7_buy_items = set_day7_buy_items.size();
        //36-43
        i_f[16] = u_day1_bahavior_cross_hours = (u_day_bahavior_max_hour[0] == -1) ? 0 : u_day_bahavior_max_hour[0] - u_day_bahavior_min_hour[0] + 1;
        i_f[17] = u_day3_bahavior_cross_hours = (u_day_bahavior_max_hour[1] == -1) ? 0 : u_day_bahavior_max_hour[1] - u_day_bahavior_min_hour[1] + 1;
        i_f[18] = u_day7_bahavior_cross_hours = (u_day_bahavior_max_hour[2] == -1) ? 0 : u_day_bahavior_max_hour[2] - u_day_bahavior_min_hour[2] + 1;
        i_f[19] = u_day14_bahavior_cross_hours = (u_day_bahavior_max_hour[3] == -1) ? 0 : u_day_bahavior_max_hour[3] - u_day_bahavior_min_hour[3] + 1;
        i_f[20] = u_day1_behavior_cross_days = (u_day_bahavior_max_dt[0] == -1) ? 0 : u_day_bahavior_max_dt[0] - u_day_bahavior_min_dt[0] + 1;
        i_f[21] = u_day3_behavior_cross_days = (u_day_bahavior_max_dt[1] == -1) ? 0 : u_day_bahavior_max_dt[1] - u_day_bahavior_min_dt[1] + 1;
        i_f[22] = u_day7_behavior_cross_days = (u_day_bahavior_max_dt[2] == -1) ? 0 : u_day_bahavior_max_dt[2] - u_day_bahavior_min_dt[2] + 1;
        i_f[23] = u_day14_behavior_cross_days = (u_day_bahavior_max_dt[3] == -1) ? 0 : u_day_bahavior_max_dt[3] - u_day_bahavior_min_dt[3] + 1;
        //44-59
        i_f[24] = u_day14_view_count = d14_sum_act[0];
        i_f[25] = u_day14_collect_count = d14_sum_act[1];
        i_f[26] = u_day14_cart_count = d14_sum_act[2];
        i_f[27] = u_day14_buy_count = d14_sum_act[3];
        i_f[28] = u_day1_view_count = d1_sum_act[0];
        i_f[29] = u_day1_collect_count = d1_sum_act[1];
        i_f[30] = u_day1_cart_count = d1_sum_act[2];
        i_f[31] = u_day1_buy_count = d1_sum_act[3];
        i_f[32] = u_day3_view_count = d3_sum_act[0];
        i_f[33] = u_day3_collect_count = d3_sum_act[1];
        i_f[34] = u_day3_cart_count = d3_sum_act[2];
        i_f[35] = u_day3_buy_count = d3_sum_act[3];
        i_f[36] = u_day7_view_count = d7_sum_act[0];
        i_f[37] = u_day7_collect_count = d7_sum_act[1];
        i_f[38] = u_day7_cart_count = d7_sum_act[2];
        i_f[39] = u_day7_buy_count = d7_sum_act[3];
        //60-67
        d_f[19] = u_day7_avg_view_days_count = u_day7_view_count / 7.0;
        d_f[20] = u_day7_avg_collect_days_count = u_day7_collect_count / 7.0;
        d_f[21] = u_day7_avg_cart_days_count = u_day7_cart_count / 7.0;
        d_f[22] = u_day7_avg_buy_days_count = u_day7_buy_count / 7.0;
        d_f[23] = u_day3_avg_view_days_count = u_day3_view_count / 3.0;
        d_f[24] = u_day3_avg_collect_days_count = u_day3_collect_count / 3.0;
        d_f[25] = u_day3_avg_cart_days_count = u_day3_cart_count / 3.0;
        d_f[26] = u_day3_avg_buy_days_count = u_day3_buy_count / 3.0;
        //68-102 
        d_f[27] = u_day14_cart_ratio_i = (double)(u_day14_cart_items ) / (u_day14_view_items + u_day14_collect_items + u_day14_cart_items + u_day14_buy_items + MC0);
        d_f[28] = u_day14_cart_ratio_log_view_i = (double)u_day14_cart_ratio_i * Math.log(u_day14_view_items + MC0 +1);
        d_f[29] = u_day14_buy_ratio_i = (double)(u_day14_buy_items ) / ( u_day14_view_items + u_day14_collect_items + u_day14_cart_items + u_day14_buy_items + MC0);
        d_f[30] = u_day14_buy_ratio_log_view_i = (double)u_day14_buy_ratio_i * Math.log(u_day14_view_items + MC0 + 1 );
        d_f[31] = u_day14_collect_ratio_i = (double)(u_day14_collect_items ) / (u_day14_view_items + u_day14_collect_items + u_day14_cart_items + u_day14_buy_items + MC0);         
        d_f[32] = u_day7_cart_ratio_i = (double)(u_day7_cart_items ) / (u_day7_view_items + u_day7_collect_items + u_day7_cart_items + u_day7_buy_items + MC0);
        d_f[33] = u_day7_cart_ratio_log_view_i = (double)u_day7_cart_ratio_i * Math.log(u_day7_view_items + MC0 + 1);
        d_f[34] = u_day7_buy_ratio_i = (double)(u_day7_buy_items ) / (u_day7_view_items + u_day7_collect_items + u_day7_cart_items + u_day7_buy_items + MC0);
        d_f[35] = u_day7_buy_ratio_log_view_i = (double)u_day7_buy_ratio_i * Math.log(u_day7_view_items + MC0+1);
        d_f[36] = u_day7_collect_ratio_i = (double)u_day7_collect_items / (u_day7_view_items + u_day7_collect_items + u_day7_cart_items + u_day7_buy_items + MC0);
        d_f[37] = u_day7_cart_ratio_a = (double)(u_day7_cart_count ) / (u_day7_view_count + u_day7_collect_count + u_day7_cart_count + u_day7_buy_count + MC0);
        d_f[38] = u_day7_cart_ratio_log_view_a = u_day7_cart_ratio_a * Math.log(u_day7_view_count + MC0 + 1);
        d_f[39] = u_day7_buy_ratio_a = (double)(u_day7_buy_count ) / (u_day7_view_count + u_day7_collect_count + u_day7_cart_count + u_day7_buy_count + MC0 );
        d_f[40] = u_day7_buy_ratio_log_view_a = u_day7_buy_ratio_a * Math.log(u_day7_view_count + MC0 + 1);
        d_f[41] = u_day7_collect_ratio_a = (double)(u_day7_collect_count ) / (u_day7_view_count + u_day7_collect_count + u_day7_cart_count + u_day7_buy_count + MC0 );      
        d_f[42] = u_day3_cart_ratio_i = (double)(u_day3_cart_items ) / (u_day3_view_items + u_day3_collect_items + u_day3_cart_items + u_day3_buy_items + MC0);
        d_f[43] = u_day3_cart_ratio_log_view_i = (double)u_day3_cart_ratio_i * Math.log(u_day3_view_items + MC0 +1);
        d_f[44] = u_day3_buy_ratio_i = (double)(u_day3_buy_items ) / (u_day3_view_items + u_day3_collect_items + u_day3_cart_items + u_day3_buy_items + MC0);
        d_f[45] = u_day3_buy_ratio_log_view_i = (double)u_day3_buy_ratio_i * Math.log(u_day3_view_items + MC0 +1);
        d_f[46] = u_day3_collect_ratio_i = (double)(u_day3_collect_items ) / (u_day3_view_items + u_day3_collect_items + u_day3_cart_items + u_day3_buy_items + MC0);
        d_f[47] = u_day3_cart_ratio_a = (double)(u_day3_cart_count ) / (u_day3_view_count + u_day3_collect_count + u_day3_cart_count + u_day3_buy_count + MC0);
        d_f[48] = u_day3_cart_ratio_log_view_a = u_day3_cart_ratio_a * Math.log(u_day3_view_count + MC0 + 1);
        d_f[49] = u_day3_buy_ratio_a = (double)(u_day3_buy_count ) / (u_day3_view_count + u_day3_collect_count + u_day3_cart_count + u_day3_buy_count + MC0);
        d_f[50] = u_day3_buy_ratio_log_view_a = u_day3_buy_ratio_a * Math.log(u_day3_view_count + MC0 +1 );
        d_f[51] = u_day3_collect_ratio_a = (double)(u_day3_collect_count ) / (u_day3_view_count + u_day3_collect_count + u_day3_cart_count + u_day3_buy_count + MC0);      
        d_f[52] = u_day1_cart_ratio_i = (double)(u_day1_cart_items ) / (u_day1_view_items + u_day1_collect_items + u_day1_cart_items + u_day1_buy_items + MC0);
        d_f[53] = u_day1_cart_ratio_log_view_i = (double)u_day1_cart_ratio_i * Math.log(u_day1_view_items + MC0 +1);
        d_f[54] = u_day1_buy_ratio_i = (double)(u_day1_buy_items ) / (u_day1_view_items + u_day1_collect_items + u_day1_cart_items + u_day1_buy_items + MC0);
        d_f[55] = u_day1_buy_ratio_log_view_i = (double)u_day1_buy_ratio_i * Math.log(u_day1_view_items + MC0 +1);
        d_f[56] = u_day1_collect_ratio_i = (double)(u_day1_collect_items ) / (u_day1_view_items + u_day1_collect_items + u_day1_cart_items + u_day1_buy_items + MC0);
        d_f[57] = u_day1_cart_ratio_a = (double)(u_day1_cart_count ) / (u_day1_view_count + u_day1_collect_count + u_day1_cart_count + u_day1_buy_count + MC0);
        d_f[58] = u_day1_cart_ratio_log_view_a = u_day1_cart_ratio_a * Math.log(u_day1_view_count + MC0 +1 );
        d_f[59] = u_day1_buy_ratio_a = (double)(u_day1_buy_count ) / (u_day1_view_count + u_day1_collect_count + u_day1_cart_count + u_day1_buy_count + MC0);
        d_f[60] = u_day1_buy_ratio_log_view_a = u_day1_buy_ratio_a * Math.log(u_day1_view_count + MC0 +1);
        d_f[61] = u_day1_collect_ratio_a = (double)(u_day1_collect_count ) / (u_day1_view_count + u_day1_collect_count + u_day1_cart_count + u_day1_buy_count + MC0);
        //103-114

        d_f[62] = u_day1_7_to_1_14_view_count_ratio = (double)(u_day7_view_count ) / ( u_day7_view_count + u_day14_view_count + MC0);
        d_f[63] = u_day1_3_to_1_7_view_count_ratio = (double)(u_day3_view_count ) / (u_day3_view_count+u_day7_view_count + MC0);
        d_f[64] = u_day1_to_1_3_view_count_ratio = (double)(u_day1_view_count ) / (u_day1_view_count+u_day3_view_count + MC0);      
        d_f[65] = u_day1_7_to_1_14_collect_count_ratio = (double)(u_day7_collect_count ) / (u_day7_collect_count +u_day14_collect_count + MC0);
        d_f[66] = u_day1_3_to_1_7_collect_count_ratio = (double)(u_day3_collect_count ) / (u_day3_collect_count + u_day7_collect_count + MC0);
        d_f[67] = u_day1_to_1_3_collect_count_ratio = (double)(u_day1_collect_count ) / (u_day1_collect_count +u_day3_collect_count + MC0);        
        d_f[68] = u_day1_7_to_1_14_cart_count_ratio = (double)(u_day7_cart_count ) / (u_day7_cart_count + u_day14_cart_count + MC0);
        d_f[69] = u_day1_3_to_1_7_cart_count_ratio = (double)(u_day3_cart_count ) / (u_day3_cart_count + u_day7_cart_count + MC0);
        d_f[70] = u_day1_to_1_3_cart_count_ratio = (double)(u_day1_cart_count ) / ( u_day1_cart_count + u_day3_cart_count + MC0);    
        d_f[71] = u_day1_7_to_1_14_buy_count_ratio = (double)(u_day7_buy_count ) / (u_day7_buy_count + u_day14_buy_count + MC0);
        d_f[72] = u_day1_3_to_1_7_buy_count_ratio = (double)(u_day3_buy_count ) / (u_day3_buy_count + u_day7_buy_count + MC0);
        d_f[73] = u_day1_to_1_3_buy_count_ratio = (double)(u_day1_buy_count ) / (u_day1_buy_count + u_day3_buy_count + MC0);
        //115-126
        d_f[74] = u_day1_7_to_1_14_view_items_ratio = (double)(u_day7_view_items ) / (u_day7_view_items + u_day14_view_items + MC0);
        d_f[75] = u_day1_3_to_1_7_view_items_ratio = (double)(u_day3_view_items ) / (u_day3_view_items + u_day7_view_items + MC0);
        d_f[76] = u_day1_to_1_3_view_items_ratio = (double)(u_day1_view_items ) / (u_day1_view_items + u_day3_view_items + MC0);        
        d_f[77] = u_day1_7_to_1_14_collect_items_ratio = (double)(u_day7_collect_items ) / (u_day7_collect_items + u_day14_collect_items + MC0);
        d_f[78] = u_day1_3_to_1_7_collect_items_ratio = (double)(u_day3_collect_items ) / (u_day3_collect_items + u_day7_collect_items + MC0);
        d_f[79] = u_day1_to_1_3_collect_items_ratio = (double)(u_day1_collect_items ) / (u_day1_collect_items + u_day3_collect_items + MC0);       
        d_f[80] = u_day1_7_to_1_14_cart_items_ratio = (double)(u_day7_cart_items ) / (u_day7_cart_items + u_day14_cart_items + MC0);
        d_f[81] = u_day1_3_to_1_7_cart_items_ratio = (double)(u_day3_cart_items ) / (u_day3_cart_items + u_day7_cart_items + MC0);
        d_f[82] = u_day1_to_1_3_cart_items_ratio = (double)(u_day1_cart_items ) / (u_day1_cart_items + u_day3_cart_items + MC0);        
        d_f[83] = u_day1_7_to_1_14_buy_items_ratio = (double)(u_day7_buy_items ) / (u_day7_buy_items + u_day14_buy_items + MC0);
        d_f[84] = u_day1_3_to_1_7_buy_items_ratio = (double)(u_day3_buy_items) / (u_day3_buy_items + u_day7_buy_items + MC0);
        d_f[85] = u_day1_to_1_3_buy_items_ratio = (double)(u_day1_buy_items ) / (u_day1_buy_items + u_day3_buy_items + MC0);
        //127-132
        d_f[86] = u_day1_7_to_1_14_behavior_cross_days_ratio = (double)(u_day7_behavior_cross_days ) / (u_day7_behavior_cross_days + u_day14_behavior_cross_days  + MC0);
        d_f[87] = u_day1_3_to_1_7_behavior_cross_days_ratio = (double)(u_day3_behavior_cross_days ) / (u_day3_behavior_cross_days + u_day7_behavior_cross_days  + MC0);
        d_f[88] = u_day1_to_1_3_behavior_cross_days_ratio = (double)(u_day1_behavior_cross_days ) / (u_day1_behavior_cross_days + u_day3_behavior_cross_days  + MC0);     
        d_f[89] = u_day1_7_to_1_14_behavior_cross_hours_ratio = (double)(u_day7_bahavior_cross_hours ) / (u_day7_bahavior_cross_hours + u_day14_bahavior_cross_hours  + MC0);
        d_f[90] = u_day1_3_to_1_7_behavior_cross_hours_ratio = (double)(u_day3_bahavior_cross_hours ) / ( u_day3_bahavior_cross_hours + u_day7_bahavior_cross_hours  + MC0);
        d_f[91] = u_day1_to_1_3_behavior_cross_hours_ratio = (double)(u_day1_bahavior_cross_hours ) / (u_day1_bahavior_cross_hours + u_day3_bahavior_cross_hours  + MC0);
        //133-152
        d_f[92] = u_day6_cart2view_items = (double)(set_day6_cart_items.size()  ) / ( set_day6_cart_items.size() + set_day6_view_items.size()  + MC0);
        d_f[93] = u_day6_cart2buy_items = (double)(set_day6_cart_items.size() ) / ( set_day6_cart_items.size() + set_day6_buy_items.size()  + MC0);
        d_f[94] = u_day6_cart2view_days = (double)(set_d6_cart_days.size() ) / ( set_d6_cart_days.size()  + set_d6_view_days.size()  + MC0);
        d_f[95] = u_day6_cart2buy_days = (double)(set_d6_cart_days.size() ) / (set_d6_cart_days.size() + set_d6_buy_days.size()  + MC0);
        d_f[96] = u_day6_cart2view_ratio = (double)(d6_sum_act[2] ) / (d6_sum_act[2] + d6_sum_act[0]  + MC0);
        d_f[97] = u_day6_cart2buy_ratio = (double)(d6_sum_act[2] ) / (d6_sum_act[2]  + d6_sum_act[3]  + MC0);
        d_f[98] = u_day6_cart2view_ratio_log_cart = u_day6_cart2view_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[99] = u_day6_cart2buy_ratio_log_cart = u_day6_cart2buy_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[100] = u_day6_collect2view_ratio = (double)(d6_sum_act[1]) / ( d6_sum_act[1] + d6_sum_act[0]  + MC0);
        d_f[101] = u_day6_collect2buy_ratio = (double)(d6_sum_act[1] ) / (d6_sum_act[1] + d6_sum_act[3]  + MC0);       
        d_f[102] = u_day2_cart2view_items = (double)(set_day2_cart_items.size()  ) / ( set_day2_cart_items.size() + set_day2_view_items.size()  + MC0);
        d_f[103] = u_day2_cart2buy_items = (double)(set_day2_cart_items.size() ) / (set_day2_cart_items.size() + set_day2_buy_items.size()  + MC0);
        d_f[104] = u_day2_cart2view_days = (double)(set_d6_cart_days.size() ) / (set_d6_cart_days.size()  + set_d6_view_days.size()  + MC0);
        d_f[105] = u_day2_cart2buy_days = (double)(set_d6_cart_days.size() ) / (set_d6_cart_days.size() + set_d6_buy_days.size()  + MC0);
        d_f[106] = u_day2_cart2view_ratio = (double)(d6_sum_act[2] ) / (d6_sum_act[2]  + d6_sum_act[0]  + MC0);
        d_f[107] = u_day2_cart2buy_ratio = (double)(d6_sum_act[2] ) / (d6_sum_act[2]  + d6_sum_act[3]  + MC0);
        d_f[108] = u_day2_cart2view_ratio_log_cart = u_day2_cart2view_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[109] = u_day2_cart2buy_ratio_log_cart = u_day2_cart2buy_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[110] = u_day2_collect2view_ratio = (double)(d6_sum_act[1] ) / (d6_sum_act[1]  + d6_sum_act[0]  + MC0);
        d_f[111] = u_day2_collect2buy_ratio = (double)(d6_sum_act[1] ) / (d6_sum_act[1] + d6_sum_act[3]  + MC0);
        result.set(0,key.get("uid"));
        int counter =1;
        for (int i = 1; i <= 40; i++)
        {
        	result.set(i,i_f[i-1]);counter++;
        }
        for (int j = 41; j <= 152; j++)
        {
        	result.set(j,d_f[j-41]);counter++;
        }
        
        // by lemon 
        lm_getFeatures(); 
        ArrayList lm_fList = new ArrayList();
        lm_fList.add(u_day28_cart_and_buy_dist_cnt) ; // u对多少个i 即发生了购物车也发生了购买 ； 
	    
        for (int i=0 ;i <4 ;i++){
	    	lm_fList.add(u_day28_tp_items[i] ) ;       //tp的item数
	    }
	    for (int i=0 ;i <4 ;i++){
	        lm_fList.add(u_day28_tp_count[i] );
	    }
	    for (int i=0 ;i <4 ;i++){
	        lm_fList.add(u_day28_avg_tp_days_count[i] );     //平均每天点击数
	    }
	    
        lm_fList.add(u_day28_cart_ratio  );          //加入购物车率
        lm_fList.add(u_day28_cart_ratio_log_view  ); //加入购物车率*log(view)
        lm_fList.add(day28_buy_ratio  );             //购买率
        lm_fList.add(day28_buy_ratio_log_view  );    //购买率*log(view)
        lm_fList.add(day28_collect_ratio ) ;         //收藏率
        lm_fList.add(u_day28_cart_ratio_i );
        lm_fList.add(u_day28_cart_ratio_log_view_i );
        lm_fList.add(u_day28_buy_ratio_i );
        lm_fList.add(u_day28_buy_ratio_log_view_i );
        lm_fList.add(u_day28_collect_ratio_i );

        lm_fList.add(u_day1_14_to_1_28_view_count_ratio );
        lm_fList.add(u_day1_14_to_1_28_collect_count_ratio );
        lm_fList.add(u_day1_14_to_1_28_cart_count_ratio );
        lm_fList.add(u_day1_14_to_1_28_buy_count_ratio );
        lm_fList.add(u_day1_14_to_1_28_view_items_ratio );
        lm_fList.add(u_day1_14_to_1_28_collect_items_ratio );
        lm_fList.add(u_day1_14_to_1_28_cart_items_ratio );
        lm_fList.add(u_day1_14_to_1_28_buy_items_ratio );
        
        for(int i =0 ; i <4 ; i++){
        	lm_fList.add(u_tp_days[i] );
        }
        lm_fList.add(u_all_days ) ;
        
        for(Iterator it = lm_fList.iterator();it.hasNext();){
            result.set( counter ,it.next());
            counter ++;
       }
        
        context.write(result);
    }
    
    private static int u_day_bahavior_max_hour[] = {-1, -1, -1, -1};//1/3/7/14;
    private static int u_day_bahavior_min_hour[] = {-1, -1, -1, -1};
    private static int u_day_bahavior_max_dt[] = {-1, -1, -1, -1};
    private static int u_day_bahavior_min_dt[] = {-1, -1, -1, -1};
    public void f_2(String iid, String ic, int tp, String ug, int hr, int dt_m, int dt_d)
    {
    	hr = (18 - dt_d) * 24 + (24 - hr);
    	//d1
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >16 + offsize)
    	{
    		//hour
    		if (u_day_bahavior_max_hour[0] == -1)
    		{
    			u_day_bahavior_max_hour[0] = u_day_bahavior_min_hour[0] = hr;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_hour[0] < hr)
    			{
    				u_day_bahavior_max_hour[0] = hr;
    			}
    			else if (u_day_bahavior_min_hour[0] > hr)
    			{
    				u_day_bahavior_min_hour[0] = hr;
    			}
    		}
    		//date
    		if (u_day_bahavior_max_dt[0] == -1)
    		{
    			u_day_bahavior_max_dt[0] = u_day_bahavior_min_dt[0] = dt_d;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_dt[0] < dt_d)
    			{
    				u_day_bahavior_max_dt[0] = dt_d;
    			}
    			else if (u_day_bahavior_min_dt[0] > dt_d)
    			{
    				u_day_bahavior_min_dt[0] = dt_d;
    			}
    		}
    	}
    	//d1-d3
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >14 + offsize)
    	{
    		//hour
    		if (u_day_bahavior_max_hour[1] == -1)
    		{
    			u_day_bahavior_max_hour[1] = u_day_bahavior_min_hour[1] = hr;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_hour[1] < hr)
    			{
    				u_day_bahavior_max_hour[1] = hr;
    			}
    			else if (u_day_bahavior_min_hour[1] > hr)
    			{
    				u_day_bahavior_min_hour[1] = hr;
    			}
    		}
    		//date
    		if (u_day_bahavior_max_dt[1] == -1)
    		{
    			u_day_bahavior_max_dt[1] = u_day_bahavior_min_dt[1] = dt_d;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_dt[1] < dt_d)
    			{
    				u_day_bahavior_max_dt[1] = dt_d;
    			}
    			else if (u_day_bahavior_min_dt[1] > dt_d)
    			{
    				u_day_bahavior_min_dt[1] = dt_d;
    			}
    		}
    	}
    	//d1-d7
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >10 + offsize)
    	{
    		//hour
    		if (u_day_bahavior_max_hour[2] == -1)
    		{
    			u_day_bahavior_max_hour[2] = u_day_bahavior_min_hour[2] = hr;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_hour[2] < hr)
    			{
    				u_day_bahavior_max_hour[2] = hr;
    			}
    			else if (u_day_bahavior_min_hour[2] > hr)
    			{
    				u_day_bahavior_min_hour[2] = hr;
    			}
    		}
    		//date
    		if (u_day_bahavior_max_dt[2] == -1)
    		{
    			u_day_bahavior_max_dt[2] = u_day_bahavior_min_dt[2] = dt_d;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_dt[2] < dt_d)
    			{
    				u_day_bahavior_max_dt[2] = dt_d;
    			}
    			else if (u_day_bahavior_min_dt[2] > dt_d)
    			{
    				u_day_bahavior_min_dt[2] = dt_d;
    			}
    		}
    	}
    	//d1-d14
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >3 + offsize)
    	{
    		if (u_day_bahavior_max_hour[3] == -1)
    		{
    			u_day_bahavior_max_hour[3] = u_day_bahavior_min_hour[3] = hr;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_hour[3] < hr)
    			{
    				u_day_bahavior_max_hour[3] = hr;
    			}
    			else if (u_day_bahavior_min_hour[3] > hr)
    			{
    				u_day_bahavior_min_hour[3] = hr;
    			}
    		}
    		//date
    		if (u_day_bahavior_max_dt[3] == -1)
    		{
    			u_day_bahavior_max_dt[3] = u_day_bahavior_min_dt[3] = dt_d;
    		}
    		else 
    		{
    			if (u_day_bahavior_max_dt[3] < dt_d)
    			{
    				u_day_bahavior_max_dt[3] = dt_d;
    			}
    			else if (u_day_bahavior_min_dt[3] > dt_d)
    			{
    				u_day_bahavior_min_dt[3] = dt_d;
    			}
    		}
    	}
    }
        
    private Set set_day1_view_items = new HashSet();
    private Set set_day1_collect_items = new HashSet();
    private Set set_day1_cart_items = new HashSet();
    private Set set_day1_buy_items = new HashSet();
    private Set set_day2_view_items = new HashSet();
    private Set set_day2_collect_items = new HashSet();
    private Set set_day2_cart_items = new HashSet();
    private Set set_day2_buy_items = new HashSet();
    private Set set_day3_view_items = new HashSet();
    private Set set_day3_collect_items = new HashSet();
    private Set set_day3_cart_items = new HashSet();
    private Set set_day3_buy_items = new HashSet();
    private Set set_day6_view_items = new HashSet();
    private Set set_day6_collect_items = new HashSet();
    private Set set_day6_cart_items = new HashSet();
    private Set set_day6_buy_items = new HashSet();
    private Set set_d6_view_days = new HashSet();
    private Set set_d6_collect_days = new HashSet();
    private Set set_d6_cart_days = new HashSet();
    private Set set_d6_buy_days = new HashSet();
    private Set set_day7_view_items = new HashSet();
    private Set set_day7_collect_items = new HashSet();
    private Set set_day7_cart_items = new HashSet();
    private Set set_day7_buy_items = new HashSet();
    private Set set_d13_view_items = new HashSet();
    private Set set_d13_collect_items = new HashSet();
    private Set set_d13_cart_items = new HashSet();
    private Set set_d13_buy_items = new HashSet();
    private Set set_d13_view_days = new HashSet();
    private Set set_d13_collect_days = new HashSet();
    private Set set_d13_cart_days = new HashSet();
    private Set set_d13_buy_days = new HashSet();
    private Set set_d14_view_items = new HashSet();
    private Set set_d14_collect_items = new HashSet();
    private Set set_d14_cart_items = new HashSet();
    private Set set_d14_buy_items = new HashSet();
    private int d1_sum_act[] = {0, 0, 0, 0};
    private int d2_sum_act[] = {0, 0, 0, 0};
    private int d3_sum_act[] = {0, 0, 0, 0};
    private int d6_sum_act[] = {0, 0, 0, 0};
    private int d7_sum_act[] = {0, 0, 0, 0};
    private int d13_sum_act[] = {0, 0, 0, 0};
    private int d14_sum_act[] = {0, 0, 0, 0};
    
    public void f_1(String iid, String ic, int tp, String ug, int hr, int dt_m, int dt_d)
    {
    	//d1
    	if (dt_m == 12 && dt_d == 17 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_day1_view_items.add(iid);
    			d1_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_day1_collect_items.add(iid);
    			d1_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_day1_cart_items.add(iid);
    			d1_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_day1_buy_items.add(iid);
    			d1_sum_act[tp -1] += 1;
    			break;
    		default:
    			break;	
    		}
    	}
    	//d1-d2
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >15 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_day2_view_items.add(iid);
    			d2_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_day2_collect_items.add(iid);
    			d2_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_day2_cart_items.add(iid);
    			d2_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_day2_buy_items.add(iid);
    			d2_sum_act[tp -1] += 1;
    			break;
    		default:
    			break;	
    		}
    	}
    	//d1-d3
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >14 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_day3_view_items.add(iid);
    			d3_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_day3_collect_items.add(iid);
    			d3_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_day3_cart_items.add(iid);
    			d3_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_day3_buy_items.add(iid);
    			d3_sum_act[tp -1] += 1;
    			break;
    		default:
    			break;	
    		}
    	}
    	//d1-d6
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >11 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_day6_view_items.add(iid);
    			set_d6_view_days.add(dt_d);
    			d6_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_day6_collect_items.add(iid);
    			set_d6_collect_days.add(dt_d);
    			d6_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_day6_cart_items.add(iid);
    			set_d6_cart_days.add(dt_d);
    			d6_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_day6_buy_items.add(iid);
    			set_d6_buy_days.add(dt_d);
    			d6_sum_act[tp -1] += 1;
    			break;
    		default:
    			break;	
    		}
    	}
    	//d1-d7
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >10 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_day7_view_items.add(iid);
    			d7_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_day7_collect_items.add(iid);
    			d7_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_day7_cart_items.add(iid);
    			d7_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_day7_buy_items.add(iid);
    			d7_sum_act[tp -1] += 1;
    			break;
    		default:
    			break;	
    		}
    	}
    	//d1-d13
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >4 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_d13_view_items.add(iid);
    			set_d13_view_days.add(dt_d);
    			d13_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_d13_collect_items.add(iid);
    			set_d13_collect_days.add(dt_d);
    			d13_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_d13_cart_items.add(iid);
    			set_d13_cart_days.add(dt_d);
    			d13_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_d13_buy_items.add(iid);
    			set_d13_buy_days.add(dt_d);
    			d13_sum_act[tp -1] += 1;
    			break;
    		}
    	}
    	//d1-d14
    	if (dt_m == 12 && dt_d < 18 + offsize && dt_d >3 + offsize)
    	{
    		switch(tp)
    		{
    		case 1:
    			set_d14_view_items.add(iid);
    			d14_sum_act[tp -1] += 1;
    			break;
    		case 2:
    			set_d14_collect_items.add(iid);
    			d14_sum_act[tp -1] += 1;
    			break;
    		case 3:
    			set_d14_cart_items.add(iid);
    			d14_sum_act[tp -1] += 1;
    			break;
    		case 4:
    			set_d14_buy_items.add(iid);
    			d14_sum_act[tp -1] += 1;
    			break;
    		}
    	}
    }
    
   
    public void init()
    {
        //（1-4）
        u_day14_view_items  = 0;		//点击的item数
        u_day14_collect_items =0;	//收藏的item数
        u_day14_cart_items  = 0;		//加入购物车的item数
        u_day14_buy_items  = 0;		//购买的item数
       
       //（5-8）
        u_day14_avg_view_days_count  = 0;		//平均每天点击数
        u_day14_avg_collect_days_count  = 0;	//平均每天收藏数
        u_day14_avg_cart_days_count  = 0;		//平均每天加入购物车数
        u_day14_avg_buy_days_count  = 0;		//平均每天购买次数
       
       //（9-13）
        u_day14_cart_ratio  = 0;			//加入购物车率
        u_day14_cart_ratio_log_view  = 0;	//加入购物车率*log(view)
        day14_buy_ratio  = 0;				//购买率
        day14_buy_ratio_log_view  = 0;	//购买率*log(view)
        day14_collect_ratio  = 0;			//收藏率
       
       //(14-23)
        u_day13_cart2view_items  = 0;		//加入购物车item数/点击item数
        u_day13_cart2buy_items  = 0;		//加入购物车item数/购买item数
        u_day13_cart2view_days  = 0;
        u_day13_cart2buy_days  = 0;
        u_day13_cart2view_ratio  = 0;
        u_day13_cart2buy_ratio  = 0;
        u_day13_cart2view_ratio_log_cart  = 0;
        u_day13_cart2buy_ratio_log_cart  = 0;
        u_day13_collect2view_ratio  = 0;
        u_day13_collect2buy_ratio  = 0;
       //24-35
        u_day1_view_items  = 0;
        u_day1_collect_items  = 0;
        u_day1_cart_items  = 0;
        u_day1_buy_items  = 0;
        u_day3_view_items  = 0;
        u_day3_collect_items  = 0;
        u_day3_cart_items  = 0;
        u_day3_buy_items  = 0;
        u_day7_view_items  = 0;
        u_day7_collect_items  = 0;
        u_day7_cart_items  = 0;
        u_day7_buy_items  = 0;
       //36-43
        u_day1_bahavior_cross_hours = 0;
        u_day3_bahavior_cross_hours = 0;
        u_day7_bahavior_cross_hours = 0;
        u_day14_bahavior_cross_hours = 0;
        u_day1_behavior_cross_days = 0;
        u_day3_behavior_cross_days = 0;
        u_day7_behavior_cross_days = 0;
        u_day14_behavior_cross_days = 0;    
       //44-59
        u_day14_view_count = 0;
        u_day14_collect_count = 0;
        u_day14_cart_count = 0;
        u_day14_buy_count = 0;
        u_day1_view_count = 0;
        u_day1_collect_count = 0;
        u_day1_cart_count = 0;
        u_day1_buy_count = 0;
        u_day3_view_count = 0;
        u_day3_collect_count = 0;
        u_day3_cart_count = 0;
        u_day3_buy_count = 0;
        u_day7_view_count = 0;
        u_day7_collect_count = 0;
        u_day7_cart_count = 0;
        u_day7_buy_count = 0;
       //60-67
        u_day7_avg_view_days_count = 0;
        u_day7_avg_collect_days_count = 0;
        u_day7_avg_cart_days_count = 0;
        u_day7_avg_buy_days_count = 0;
        u_day3_avg_view_days_count = 0;
        u_day3_avg_collect_days_count = 0;
        u_day3_avg_cart_days_count = 0;
        u_day3_avg_buy_days_count = 0;
       //68-102
        u_day14_cart_ratio_i = 0;
        u_day14_cart_ratio_log_view_i = 0;
        u_day14_buy_ratio_i = 0;
        u_day14_buy_ratio_log_view_i = 0;
        u_day14_collect_ratio_i = 0;
        u_day7_cart_ratio_i = 0;
        u_day7_cart_ratio_log_view_i = 0;
        u_day7_buy_ratio_i = 0;
        u_day7_buy_ratio_log_view_i = 0;
        u_day7_collect_ratio_i = 0;
        u_day7_cart_ratio_a = 0;
        u_day7_cart_ratio_log_view_a = 0;
        u_day7_buy_ratio_a = 0;
        u_day7_buy_ratio_log_view_a = 0;
        u_day7_collect_ratio_a = 0;   
        u_day3_cart_ratio_i = 0;
        u_day3_cart_ratio_log_view_i = 0;
        u_day3_buy_ratio_i = 0;
        u_day3_buy_ratio_log_view_i = 0;
        u_day3_collect_ratio_i = 0;
        u_day3_cart_ratio_a = 0;
        u_day3_cart_ratio_log_view_a = 0;
        u_day3_buy_ratio_a = 0;
        u_day3_buy_ratio_log_view_a = 0;
        u_day3_collect_ratio_a = 0;
        u_day1_cart_ratio_i = 0;
        u_day1_cart_ratio_log_view_i = 0;
        u_day1_buy_ratio_i = 0;
        u_day1_buy_ratio_log_view_i = 0;
        u_day1_collect_ratio_i = 0;
        u_day1_cart_ratio_a = 0;
        u_day1_cart_ratio_log_view_a = 0;
        u_day1_buy_ratio_a = 0;
        u_day1_buy_ratio_log_view_a = 0;
        u_day1_collect_ratio_a = 0;
       //103-114
        u_day1_7_to_1_14_view_count_ratio = 0;
        u_day1_3_to_1_7_view_count_ratio = 0;
        u_day1_to_1_3_view_count_ratio = 0;
        u_day1_7_to_1_14_collect_count_ratio = 0;
        u_day1_3_to_1_7_collect_count_ratio = 0;
        u_day1_to_1_3_collect_count_ratio = 0;
        u_day1_7_to_1_14_cart_count_ratio = 0;
        u_day1_3_to_1_7_cart_count_ratio = 0;
        u_day1_to_1_3_cart_count_ratio = 0;
        u_day1_7_to_1_14_buy_count_ratio = 0;
        u_day1_3_to_1_7_buy_count_ratio = 0;
        u_day1_to_1_3_buy_count_ratio = 0;
       //115-126
        u_day1_7_to_1_14_view_items_ratio = 0;
        u_day1_3_to_1_7_view_items_ratio = 0;
        u_day1_to_1_3_view_items_ratio = 0;    
        u_day1_7_to_1_14_collect_items_ratio = 0;
        u_day1_3_to_1_7_collect_items_ratio = 0;
        u_day1_to_1_3_collect_items_ratio = 0;    
        u_day1_7_to_1_14_cart_items_ratio = 0;
        u_day1_3_to_1_7_cart_items_ratio = 0;
        u_day1_to_1_3_cart_items_ratio = 0;    
        u_day1_7_to_1_14_buy_items_ratio = 0;
        u_day1_3_to_1_7_buy_items_ratio = 0;
        u_day1_to_1_3_buy_items_ratio = 0;
       //127-132
        u_day1_7_to_1_14_behavior_cross_days_ratio = 0;
        u_day1_3_to_1_7_behavior_cross_days_ratio = 0;
        u_day1_to_1_3_behavior_cross_days_ratio = 0;
        u_day1_7_to_1_14_behavior_cross_hours_ratio = 0;
        u_day1_3_to_1_7_behavior_cross_hours_ratio = 0;
        u_day1_to_1_3_behavior_cross_hours_ratio = 0;
       //133-152
        u_day6_cart2view_items = 0;
        u_day6_cart2buy_items = 0;
        u_day6_cart2view_days = 0;
        u_day6_cart2buy_days = 0;
        u_day6_cart2view_ratio = 0;
        u_day6_cart2buy_ratio = 0;
        u_day6_cart2view_ratio_log_cart = 0;
        u_day6_cart2buy_ratio_log_cart = 0;
        u_day6_collect2view_ratio = 0;
        u_day6_collect2buy_ratio = 0;
        u_day2_cart2view_items = 0;
        u_day2_cart2buy_items = 0;
        u_day2_cart2view_days = 0;
        u_day2_cart2buy_days = 0;
        u_day2_cart2view_ratio = 0;
        u_day2_cart2buy_ratio = 0;
        u_day2_cart2view_ratio_log_cart = 0;
        u_day2_cart2buy_ratio_log_cart = 0;
        u_day2_collect2view_ratio = 0;
        u_day2_collect2buy_ratio = 0;
        
        MC0 = 0.00001;
        
    	for (int i = 0; i < 4; i++)
    	{
    		u_day_bahavior_max_hour[i] = -1; 
        	u_day_bahavior_min_hour[i] = -1;
        	u_day_bahavior_max_dt[i] = -1;
        	u_day_bahavior_min_dt[i] = -1;
        	d1_sum_act[i] = 0;
        	d2_sum_act[i] = 0;
        	d3_sum_act[i] = 0;
        	d6_sum_act[i] = 0;
        	d7_sum_act[i] = 0;
        	d13_sum_act[i] = 0;
        	d14_sum_act[i] = 0;       	
    	}
    	set_day1_view_items.clear();
    	set_day1_collect_items.clear();
        set_day1_cart_items.clear();
        set_day1_buy_items.clear();
        set_day2_view_items.clear();
        set_day2_collect_items.clear();
        set_day2_cart_items.clear();
        set_day2_buy_items.clear();
        set_day3_view_items.clear();
        set_day3_collect_items.clear();
        set_day3_cart_items.clear();
        set_day3_buy_items.clear();
        set_day6_view_items.clear();
        set_day6_collect_items.clear();
        set_day6_cart_items.clear();
        set_day6_buy_items.clear();
        set_d6_view_days.clear();
        set_d6_collect_days.clear();
        set_d6_cart_days.clear();
        set_d6_buy_days.clear();
        set_day7_view_items.clear();
        set_day7_collect_items.clear();
        set_day7_cart_items.clear();
        set_day7_buy_items.clear();
        set_d13_view_items.clear();
        set_d13_collect_items.clear();
        set_d13_cart_items.clear();
        set_d13_buy_items.clear();
        set_d13_view_days.clear();
        set_d13_collect_days.clear();
        set_d13_cart_days.clear();
        set_d13_buy_days.clear();
        set_d14_view_items.clear();
        set_d14_collect_items.clear();
        set_d14_cart_items.clear();
        set_d14_buy_items.clear();
        
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
    
    // by lemon
    // lm_f1
    private int u_day28_cart_and_buy_dist_cnt  ; // u对多少个i 即发生了购物车也发生了购买 ； 
    // lm_f2-f5
    private int[] u_day28_tp_items ;       //点击的item数,//收藏的item数
    private Set<String>[] u_day28_tp_items_set  ;
    // lm_f6-f9
    private int[] u_day28_tp_count;
    // lm_f10-f13
    private double[] u_day28_avg_tp_days_count ;     //平均每天点击数
    // lm_f14-f18
    private double u_day28_cart_ratio ;          //加入购物车率
    private double u_day28_cart_ratio_log_view ; //加入购物车率*log(view)
    private double day28_buy_ratio ;             //购买率
    private double day28_buy_ratio_log_view ;    //购买率*log(view)
    private double day28_collect_ratio ;         //收藏率
    // lm_f19-f23
    private double u_day28_cart_ratio_i;
    private double u_day28_cart_ratio_log_view_i;
    private double u_day28_buy_ratio_i;
    private double u_day28_buy_ratio_log_view_i;
    private double u_day28_collect_ratio_i;
    // lm_f24-f27
    private double u_day1_14_to_1_28_view_count_ratio;
    private double u_day1_14_to_1_28_collect_count_ratio;
    private double u_day1_14_to_1_28_cart_count_ratio;
    private double u_day1_14_to_1_28_buy_count_ratio;
    // lm_f28-f31
    private double u_day1_14_to_1_28_view_items_ratio;
    private double u_day1_14_to_1_28_collect_items_ratio;
    private double u_day1_14_to_1_28_cart_items_ratio;
    private double u_day1_14_to_1_28_buy_items_ratio;
    // lm_f32-f35
    private int[] u_tp_days ;
    // lm_f36
    private int   u_all_days ;
    private int[][] tpDistributionInDate ;
    
    public void lm_makingFeatures(String iid ,String ic, int tp,String ug,int d, int hr){
    	if( d <= 28) {
    		u_day28_tp_items_set[tp-1].add(iid);
    		u_day28_tp_count[tp-1] ++;
    	}
    	
    	tpDistributionInDate[tp-1][d-1] += 1 ;
    	
    }
    public void lm_getFeatures(){
    	
    	Set <String >u_day28_cart_and_buy_i_temp = new HashSet<String>( u_day28_tp_items_set[2] );
//    	System.out.println(u_day28_tp_items_set[2].toString());
    	u_day28_cart_and_buy_i_temp.retainAll(u_day28_tp_items_set[3] );
//    	System.out.println(u_day28_tp_items_set[2].toString());
    	u_day28_cart_and_buy_dist_cnt = u_day28_cart_and_buy_i_temp.size();
    	
    	int sum =0 ;
    	Set<String> allItemsSet= new HashSet<String>();
    	for(int i =0 ;i <4; i ++){
    		u_day28_tp_items[i] = u_day28_tp_items_set[i].size();
    		u_day28_avg_tp_days_count[i] = u_day28_tp_count[i]*1.0 / 28 ;
    		sum += u_day28_tp_count[i];
    		allItemsSet.addAll(u_day28_tp_items_set[i]) ;
    	}
    	int allItemSetSize = allItemsSet.size();
    	
    	u_day28_cart_ratio  = u_day28_tp_count[2]*1.0 / ( sum +MC0 );          //加入购物车率
        u_day28_cart_ratio_log_view  = u_day28_cart_ratio * Math.log(u_day28_tp_count[0] +MC0); //加入购物车率*log(view)
        day28_buy_ratio  = u_day28_tp_count[3] *1.0 /( sum +MC0 ) ;             //购买率
        day28_buy_ratio_log_view  = day28_buy_ratio * Math.log(u_day28_tp_count[0] +MC0) ;    //购买率*log(view)
        day28_collect_ratio = u_day28_tp_count[1]*1.0 / ( sum +MC0 ) ;         //收藏率
        u_day28_cart_ratio_i = u_day28_tp_items_set[2].size() *1.0 / (allItemSetSize+ MC0 );
        u_day28_cart_ratio_log_view_i = u_day28_cart_ratio_i * Math.log(  u_day28_tp_items_set[0].size() + MC0 );
        u_day28_buy_ratio_i = u_day28_tp_items_set[3].size() * 1.0 / (allItemSetSize+ MC0 );
        u_day28_buy_ratio_log_view_i = u_day28_buy_ratio_i * Math.log(u_day28_tp_items_set[0].size() + MC0);
        u_day28_collect_ratio_i =  u_day28_tp_items_set[2].size() * 1.0 / (allItemSetSize+ MC0 );
    	
        u_day1_14_to_1_28_view_count_ratio = u_day14_view_count*1.0  / (u_day28_tp_count[0] + MC0);
        u_day1_14_to_1_28_collect_count_ratio = u_day14_collect_count*1.0  / (u_day28_tp_count[1] + MC0);
        u_day1_14_to_1_28_cart_count_ratio = u_day14_cart_count*1.0  / (u_day28_tp_count[2] + MC0);
        u_day1_14_to_1_28_buy_count_ratio = u_day14_buy_count*1.0  / (u_day28_tp_count[3] + MC0);
       
        u_day1_14_to_1_28_view_items_ratio =  u_day14_view_items *1.0 /( u_day28_tp_items_set[0].size()  + MC0);
        u_day1_14_to_1_28_collect_items_ratio = u_day14_collect_items *1.0 /(u_day28_tp_items_set[1].size() + MC0);
        u_day1_14_to_1_28_cart_items_ratio = u_day14_cart_items *1.0 /(u_day28_tp_items_set[2].size() + MC0);
        u_day1_14_to_1_28_buy_items_ratio = u_day14_buy_items *1.0 /( u_day28_tp_items_set[3].size() +MC0 );
        
        for(int i =0 ;i <29 + offsize ;i++){
        	if ( tpDistributionInDate[0][i] > 0  ) u_tp_days[0] ++ ;
        	if ( tpDistributionInDate[1][i] > 0  ) u_tp_days[1] ++ ;
        	if ( tpDistributionInDate[2][i] > 0  ) u_tp_days[2] ++ ;
        	if ( tpDistributionInDate[3][i] > 0  ) u_tp_days[3] ++ ;
        	
        	if ( tpDistributionInDate[0][i] > 0 || tpDistributionInDate[1][i] > 0 ||
        		tpDistributionInDate[2][i] > 0 || tpDistributionInDate[3][i] > 0 
        			) u_all_days ++ ;
        	
        }
         
        
    }
    
    public void lm_init(){
    	u_day28_cart_and_buy_dist_cnt  = 0 ; // u对多少个i 即发生了购物车也发生了购买 ； 
    	    
        u_day28_tp_items = new int[4] ;       //tp的item数
         
        u_day28_tp_items_set = new HashSet[4] ;
        for(int i =0 ;i <4;i++){
        	 u_day28_tp_items_set[i] = new HashSet();
         }
         
        u_day28_tp_count = new int[4];
       
        u_day28_avg_tp_days_count = new double[4];     //平均每天点击数

        u_day28_cart_ratio  = 0;          //加入购物车率
        u_day28_cart_ratio_log_view  = 0; //加入购物车率*log(view)
        day28_buy_ratio  = 0;             //购买率
        day28_buy_ratio_log_view  = 0;    //购买率*log(view)
        day28_collect_ratio = 0 ;         //收藏率
        u_day28_cart_ratio_i = 0;
        u_day28_cart_ratio_log_view_i = 0;
        u_day28_buy_ratio_i = 0;
        u_day28_buy_ratio_log_view_i = 0;
        u_day28_collect_ratio_i = 0;

        u_day1_14_to_1_28_view_count_ratio = 0;
        u_day1_14_to_1_28_collect_count_ratio = 0;
        u_day1_14_to_1_28_cart_count_ratio = 0;
        u_day1_14_to_1_28_buy_count_ratio = 0;
       
        u_day1_14_to_1_28_view_items_ratio = 0;
        u_day1_14_to_1_28_collect_items_ratio = 0;
        u_day1_14_to_1_28_cart_items_ratio = 0;
        u_day1_14_to_1_28_buy_items_ratio = 0;
        
        u_tp_days  = new int[4];
        u_all_days  = 0 ;
        tpDistributionInDate = new int [4][31] ; // 每种行为在时间上的分布
    	
    }
    
}
