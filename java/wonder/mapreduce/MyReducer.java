package wonder.mapreduce;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Reducer模板。请用真实逻辑替换模板内容
 */
public class MyReducer implements Reducer {
    private Record result;
    
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

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
    
    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException 
    {
    	long count = 0;
        int offsize = 1;
        Long[] ui_h5fea = new Long[]{0L, 0L, 0L, 0L};
        Long[] ui_h10fea = new Long[]{0L, 0L, 0L, 0L};
        Long[] ui_d1fea = new Long[]{0L, 0L, 0L, 0L};
        Long[] ui_d2fea = new Long[]{0L, 0L, 0L, 0L};
        Long[] ui_d3fea = new Long[]{0L, 0L, 0L, 0L};
        Long[] ui_d5fea = new Long[]{0L, 0L, 0L, 0L};
              
      //遍历key的所有value，并累加
        while (values.hasNext()) {
            Record val = values.next();    
            int tp = 0;
            int hr = 0;
            String dt = "dtdt";
            String ug = "ugug";  
            
            tp = new Long(val.getBigint("tp")).intValue();
            dt = val.getString("dt");
            hr = new Long(val.getBigint("hr")).intValue();
            String[] parts = dt.split("-");
            int dt1 = Integer.parseInt(parts[0]);
            int dt2 = Integer.parseInt(parts[1]);
            
            //d1
            if (dt2 > (16 + offsize) && dt2 < (18 + offsize)){
            	if (hr>18){
            		ui_h5fea[tp-1]++;
            	}
            	if (hr>13){
            		ui_h10fea[tp-1]++;
            	}
            	ui_d1fea[tp-1]++;
            }
            //d2
            if (dt2 > (15 + offsize) && dt2 < (18 + offsize))
            {
            	ui_d2fea[tp-1]++;
            }
            //d3
            if (dt2 > (14 + offsize) && dt2 < (18 + offsize))
            {
            	ui_d3fea[tp-1]++;
            }
            //d5
            if (dt2 > (12 + offsize) && dt2 < (18 + offsize))
            {
            	ui_d5fea[tp-1]++;
            }
        }
        Long ui_h5act = ui_h5fea[0] + ui_h5fea[1] + ui_h5fea[2] + ui_h5fea[3];
        Long ui_h10act = ui_h10fea[0] + ui_h10fea[1] + ui_h10fea[2] + ui_h10fea[3];
        Long ui_d1act = ui_d1fea[0] + ui_d1fea[1] + ui_d1fea[2] + ui_d1fea[3];
        Long ui_d2act = ui_d2fea[0] + ui_d2fea[1] + ui_d2fea[2] + ui_d2fea[3];
        Long ui_d3act = ui_d3fea[0] + ui_d3fea[1] + ui_d3fea[2] + ui_d3fea[3];
        Long ui_d5act = ui_d5fea[0] + ui_d5fea[1] + ui_d5fea[2] + ui_d5fea[3];
         
        result.set(0,key.getString(0));
        result.set(1,key.getString(1));
        result.set(2,key.getString(2));
        result.set(3,ui_h5fea[0]);
        result.set(4,ui_h5fea[1]);
        result.set(5,ui_h5fea[2]);
        result.set(6,ui_h5fea[3]);        
        result.set(7,ui_h10fea[0]);
        result.set(8,ui_h10fea[1]);
        result.set(9,ui_h10fea[2]);
        result.set(10,ui_h10fea[3]);
        result.set(11,ui_d1fea[0]);
        result.set(12,ui_d1fea[1]);
        result.set(13,ui_d1fea[2]);
        result.set(14,ui_d1fea[3]);        
        result.set(15,ui_d2fea[0]);
        result.set(16,ui_d2fea[1]);
        result.set(17,ui_d2fea[2]);
        result.set(18,ui_d2fea[3]);        
        result.set(19,ui_d3fea[0]);
        result.set(20,ui_d3fea[1]);
        result.set(21,ui_d3fea[2]);
        result.set(22,ui_d3fea[3]);        
        result.set(23,ui_d5fea[0]);
        result.set(24,ui_d5fea[1]);
        result.set(25,ui_d5fea[2]);
        result.set(26,ui_d5fea[3]);      
        result.set(27,ui_h5act);
        result.set(28,ui_h10act);
        result.set(29,ui_d1act);
        result.set(30,ui_d2act);
        result.set(31,ui_d3act);
        result.set(32,ui_d5act);
        result.set(33, (double)(ui_d5fea[3] +  + 0.0001) / (ui_d5act + 0.0001));

        context.write(result);
    }
    
    public void reduce_user(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        long count = 0; 
        init();
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
            int dt_m = Integer.parseInt(parts[0]);
            int dt_d = Integer.parseInt(parts[1]);
            
            f_1(iid, ic, tp, ug, hr,dt_m, dt_d); 
            f_2(iid, ic, tp, ug, hr,dt_m, dt_d);
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
        d_f[4] = u_day14_cart_ratio = ((double)(d14_sum_act[2] + 0.0001) / (d14_sum_act[0] + d14_sum_act[1] + d14_sum_act[2] + d14_sum_act[3] + 0.0001));
        d_f[5] = u_day14_cart_ratio_log_view = u_day14_cart_ratio * Math.log(d14_sum_act[0] + 0.0001);
        d_f[6] = day14_buy_ratio = (double)((double)(d14_sum_act[3] + 0.0001) / (d14_sum_act[0] + d14_sum_act[1] + d14_sum_act[2] + d14_sum_act[3] + 0.0001));
        d_f[7] = day14_buy_ratio_log_view = day14_buy_ratio * Math.log(d14_sum_act[0] + 0.0001);
        d_f[8] = day14_collect_ratio = (double)((double)(d14_sum_act[1] + 0.0001) / (d14_sum_act[0] + d14_sum_act[1] + d14_sum_act[2] + d14_sum_act[3] + 0.0001));
        //14-23
        d_f[9] = u_day13_cart2view_items = (double)(d13_sum_act[2] + 0.0001) / (d13_sum_act[0] + 0.0001);		
        d_f[10] = u_day13_cart2buy_items = (double)(d13_sum_act[2] + 0.0001) / (d13_sum_act[3] + 0.0001);		
        d_f[11] = u_day13_cart2view_days = (double)(set_d13_cart_days.size() + 0.0001) / (set_d13_view_days.size() + 0.0001);
        d_f[12] = u_day13_cart2buy_days = (double)(set_d13_cart_days.size() + 0.0001) / (set_d13_buy_days.size() + 0.0001);
        d_f[13] = u_day13_cart2view_ratio = (double)(d13_sum_act[2] + 0.0001) / (d13_sum_act[0] + 0.0001);
        d_f[14] = u_day13_cart2buy_ratio = (double)(d13_sum_act[2] + 0.0001) / (d13_sum_act[3] + 0.0001);
        d_f[15] = u_day13_cart2view_ratio_log_cart = u_day13_cart2view_ratio * Math.log(d13_sum_act[2] + 0.0001);
        d_f[16] = u_day13_cart2buy_ratio_log_cart = u_day13_cart2buy_ratio * Math.log(d13_sum_act[2] + 0.0001);
        d_f[17] = u_day13_collect2view_ratio = (double)(d13_sum_act[1] + 0.0001) / (d13_sum_act[0] + 0.0001);
        d_f[18] = u_day13_collect2buy_ratio = (double)(d13_sum_act[1] + 0.0001) / (d13_sum_act[3] + 0.0001);
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
        d_f[27] = u_day14_cart_ratio_i = (double)(u_day14_cart_items + 0.0001) / (u_day14_view_items + u_day14_collect_items + u_day14_cart_items + u_day14_buy_items + 0.0001);
        d_f[28] = u_day14_cart_ratio_log_view_i = (double)u_day14_cart_ratio_i * Math.log(u_day14_view_items + 0.0001);
        d_f[29] = u_day14_buy_ratio_i = (double)(u_day14_buy_items + 0.0001) / (u_day14_view_items + u_day14_collect_items + u_day14_cart_items + u_day14_buy_items + 0.0001);
        d_f[30] = u_day14_buy_ratio_log_view_i = (double)u_day14_buy_ratio_i * Math.log(u_day14_view_items + 0.0001);
        d_f[31] = u_day14_collect_ratio_i = (double)(u_day14_collect_items + 0.0001) / (u_day14_view_items + u_day14_collect_items + u_day14_cart_items + u_day14_buy_items + 0.0001);         
        d_f[32] = u_day7_cart_ratio_i = (double)(u_day7_cart_items + 0.0001) / (u_day7_view_items + u_day7_collect_items + u_day7_cart_items + u_day7_buy_items + 0.0001);
        d_f[33] = u_day7_cart_ratio_log_view_i = (double)u_day7_cart_ratio_i * Math.log(u_day7_view_items + 0.0001);
        d_f[34] = u_day7_buy_ratio_i = (double)(u_day7_buy_items + 0.0001) / (u_day7_view_items + u_day7_collect_items + u_day7_cart_items + u_day7_buy_items + 0.0001);
        d_f[35] = u_day7_buy_ratio_log_view_i = (double)u_day7_buy_ratio_i * Math.log(u_day7_view_items + 0.0001);
        d_f[36] = u_day7_collect_ratio_i = (double)u_day7_collect_items / (u_day7_view_items + u_day7_collect_items + u_day7_cart_items + u_day7_buy_items);
        d_f[37] = u_day7_cart_ratio_a = (double)(u_day7_cart_count + 0.0001) / (u_day7_view_count + u_day7_collect_count + u_day7_cart_count + u_day7_buy_count + 0.0001);
        d_f[38] = u_day7_cart_ratio_log_view_a = u_day7_cart_ratio_a * Math.log(u_day7_view_count + 0.0001);
        d_f[39] = u_day7_buy_ratio_a = (double)(u_day7_buy_count + 0.0001) / (u_day7_view_count + u_day7_collect_count + u_day7_cart_count + u_day7_buy_count + 0.0001);
        d_f[40] = u_day7_buy_ratio_log_view_a = u_day7_buy_ratio_a * Math.log(u_day7_view_count + 0.0001);
        d_f[41] = u_day7_collect_ratio_a = (double)(u_day7_collect_count + 0.0001) / (u_day7_view_count + u_day7_collect_count + u_day7_cart_count + u_day7_buy_count + 0.0001);      
        d_f[42] = u_day3_cart_ratio_i = (double)(u_day3_cart_items + 0.0001) / (u_day3_view_items + u_day3_collect_items + u_day3_cart_items + u_day3_buy_items + 0.0001);
        d_f[43] = u_day3_cart_ratio_log_view_i = (double)u_day3_cart_ratio_i * Math.log(u_day3_view_items + 0.0001);
        d_f[44] = u_day3_buy_ratio_i = (double)(u_day3_buy_items + 0.0001) / (u_day3_view_items + u_day3_collect_items + u_day3_cart_items + u_day3_buy_items + 0.0001);
        d_f[45] = u_day3_buy_ratio_log_view_i = (double)u_day3_buy_ratio_i * Math.log(u_day3_view_items + 0.0001);
        d_f[46] = u_day3_collect_ratio_i = (double)(u_day3_collect_items + 0.0001) / (u_day3_view_items + u_day3_collect_items + u_day3_cart_items + u_day3_buy_items + 0.0001);
        d_f[47] = u_day3_cart_ratio_a = (double)(u_day3_cart_count + 0.0001) / (u_day3_view_count + u_day3_collect_count + u_day3_cart_count + u_day3_buy_count + 0.0001);
        d_f[48] = u_day3_cart_ratio_log_view_a = u_day3_cart_ratio_a * Math.log(u_day3_view_count + 0.0001);
        d_f[49] = u_day3_buy_ratio_a = (double)(u_day3_buy_count + 0.0001) / (u_day3_view_count + u_day3_collect_count + u_day3_cart_count + u_day3_buy_count + 0.0001);
        d_f[50] = u_day3_buy_ratio_log_view_a = u_day3_buy_ratio_a * Math.log(u_day3_view_count + 0.0001);
        d_f[51] = u_day3_collect_ratio_a = (double)(u_day3_collect_count + 0.0001) / (u_day3_view_count + u_day3_collect_count + u_day3_cart_count + u_day3_buy_count + 0.0001);      
        d_f[52] = u_day1_cart_ratio_i = (double)(u_day1_cart_items + 0.0001) / (u_day1_view_items + u_day1_collect_items + u_day1_cart_items + u_day1_buy_items + 0.0001);
        d_f[53] = u_day1_cart_ratio_log_view_i = (double)u_day1_cart_ratio_i * Math.log(u_day1_view_items + 0.0001);
        d_f[54] = u_day1_buy_ratio_i = (double)(u_day1_buy_items + 0.0001) / (u_day1_view_items + u_day1_collect_items + u_day1_cart_items + u_day1_buy_items + 0.0001);
        d_f[55] = u_day1_buy_ratio_log_view_i = (double)u_day1_buy_ratio_i * Math.log(u_day1_view_items + 0.0001);
        d_f[56] = u_day1_collect_ratio_i = (double)(u_day1_collect_items + 0.0001) / (u_day1_view_items + u_day1_collect_items + u_day1_cart_items + u_day1_buy_items + 0.0001);
        d_f[57] = u_day1_cart_ratio_a = (double)(u_day1_cart_count + 0.0001) / (u_day1_view_count + u_day1_collect_count + u_day1_cart_count + u_day1_buy_count + 0.0001);
        d_f[58] = u_day1_cart_ratio_log_view_a = u_day1_cart_ratio_a * Math.log(u_day1_view_count + 0.0001);
        d_f[59] = u_day1_buy_ratio_a = (double)(u_day1_buy_count + 0.0001) / (u_day1_view_count + u_day1_collect_count + u_day1_cart_count + u_day1_buy_count + 0.0001);
        d_f[60] = u_day1_buy_ratio_log_view_a = u_day1_buy_ratio_a * Math.log(u_day1_view_count + 0.0001);
        d_f[61] = u_day1_collect_ratio_a = (double)(u_day1_collect_count + 0.0001) / (u_day1_view_count + u_day1_collect_count + u_day1_cart_count + u_day1_buy_count + 0.0001);
        //103-114
        d_f[62] = u_day1_7_to_1_14_view_count_ratio = (double)(u_day7_view_count + 0.0001) / (u_day14_view_count + 0.0001);
        d_f[63] = u_day1_3_to_1_7_view_count_ratio = (double)(u_day3_view_count + 0.0001) / (u_day7_view_count + 0.0001);
        d_f[64] = u_day1_to_1_3_view_count_ratio = (double)(u_day1_view_count + 0.0001) / (u_day3_view_count + 0.0001);      
        d_f[65] = u_day1_7_to_1_14_collect_count_ratio = (double)(u_day7_collect_count + 0.0001) / (u_day14_collect_count + 0.0001);
        d_f[66] = u_day1_3_to_1_7_collect_count_ratio = (double)(u_day3_collect_count + 0.0001) / (u_day7_collect_count + 0.0001);
        d_f[67] = u_day1_to_1_3_collect_count_ratio = (double)(u_day1_collect_count + 0.0001) / (u_day3_collect_count + 0.0001);        
        d_f[68] = u_day1_7_to_1_14_cart_count_ratio = (double)(u_day7_cart_count + 0.0001) / (u_day14_cart_count + 0.0001);
        d_f[69] = u_day1_3_to_1_7_cart_count_ratio = (double)(u_day3_cart_count + 0.0001) / (u_day7_cart_count + 0.0001);
        d_f[70] = u_day1_to_1_3_cart_count_ratio = (double)(u_day1_cart_count + 0.0001) / (u_day3_cart_count + 0.0001);    
        d_f[71] = u_day1_7_to_1_14_buy_count_ratio = (double)(u_day7_buy_count + 0.0001) / (u_day14_buy_count + 0.0001);
        d_f[72] = u_day1_3_to_1_7_buy_count_ratio = (double)(u_day3_buy_count + 0.0001) / (u_day7_buy_count + 0.0001);
        d_f[73] = u_day1_to_1_3_buy_count_ratio = (double)(u_day1_buy_count + 0.0001) / (u_day3_buy_count + 0.0001);
        //115-126
        d_f[74] = u_day1_7_to_1_14_view_items_ratio = (double)(u_day7_view_items + 0.0001) / (u_day14_view_items + 0.0001);
        d_f[75] = u_day1_3_to_1_7_view_items_ratio = (double)(u_day3_view_items + 0.0001) / (u_day7_view_items + 0.0001);
        d_f[76] = u_day1_to_1_3_view_items_ratio = (double)(u_day1_view_items + 0.0001) / (u_day3_view_items + 0.0001);        
        d_f[77] = u_day1_7_to_1_14_collect_items_ratio = (double)(u_day7_collect_items + 0.0001) / (u_day14_collect_items + 0.0001);
        d_f[78] = u_day1_3_to_1_7_collect_items_ratio = (double)(u_day3_collect_items + 0.0001) / (u_day7_collect_items + 0.0001);
        d_f[79] = u_day1_to_1_3_collect_items_ratio = (double)(u_day1_collect_items + 0.0001) / (u_day3_collect_items + 0.0001);       
        d_f[80] = u_day1_7_to_1_14_cart_items_ratio = (double)(u_day7_cart_items + 0.0001) / (u_day14_cart_items + 0.0001);
        d_f[81] = u_day1_3_to_1_7_cart_items_ratio = (double)(u_day3_cart_items + 0.0001) / (u_day7_cart_items + 0.0001);
        d_f[82] = u_day1_to_1_3_cart_items_ratio = (double)(u_day1_cart_items + 0.0001) / (u_day3_cart_items + 0.0001);        
        d_f[83] = u_day1_7_to_1_14_buy_items_ratio = (double)(u_day7_buy_items + 0.0001) / (u_day14_buy_items + 0.0001);
        d_f[84] = u_day1_3_to_1_7_buy_items_ratio = (double)(u_day3_buy_items + 0.0001) / (u_day7_buy_items + 0.0001);
        d_f[85] = u_day1_to_1_3_buy_items_ratio = (double)(u_day1_buy_items + 0.0001) / (u_day3_buy_items + 0.0001);
        //127-132
        d_f[86] = u_day1_7_to_1_14_behavior_cross_days_ratio = (double)(u_day7_behavior_cross_days  + 0.0001) / (u_day14_behavior_cross_days  + 0.0001);
        d_f[87] = u_day1_3_to_1_7_behavior_cross_days_ratio = (double)(u_day3_behavior_cross_days  + 0.0001) / (u_day7_behavior_cross_days  + 0.0001);
        d_f[88] = u_day1_to_1_3_behavior_cross_days_ratio = (double)(u_day1_behavior_cross_days  + 0.0001) / (u_day3_behavior_cross_days  + 0.0001);     
        d_f[89] = u_day1_7_to_1_14_behavior_cross_hours_ratio = (double)(u_day7_bahavior_cross_hours  + 0.0001) / (u_day14_bahavior_cross_hours  + 0.0001);
        d_f[90] = u_day1_3_to_1_7_behavior_cross_hours_ratio = (double)(u_day3_bahavior_cross_hours  + 0.0001) / (u_day7_bahavior_cross_hours  + 0.0001);
        d_f[91] = u_day1_to_1_3_behavior_cross_hours_ratio = (double)(u_day1_bahavior_cross_hours  + 0.0001) / (u_day3_bahavior_cross_hours  + 0.0001);
        //133-152
        d_f[92] = u_day6_cart2view_items = (double)(set_day6_cart_items.size()  + 0.0001) / (set_day6_view_items.size()  + 0.0001);
        d_f[93] = u_day6_cart2buy_items = (double)(set_day6_cart_items.size()  + 0.0001) / (set_day6_buy_items.size()  + 0.0001);
        d_f[94] = u_day6_cart2view_days = (double)(set_d6_cart_days.size()  + 0.0001) / (set_d6_view_days.size()  + 0.0001);
        d_f[95] = u_day6_cart2buy_days = (double)(set_d6_cart_days.size()  + 0.0001) / (set_d6_buy_days.size()  + 0.0001);
        d_f[96] = u_day6_cart2view_ratio = (double)(d6_sum_act[2]  + 0.0001) / (d6_sum_act[0]  + 0.0001);
        d_f[97] = u_day6_cart2buy_ratio = (double)(d6_sum_act[2]  + 0.0001) / (d6_sum_act[3]  + 0.0001);
        d_f[98] = u_day6_cart2view_ratio_log_cart = u_day6_cart2view_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[99] = u_day6_cart2buy_ratio_log_cart = u_day6_cart2buy_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[100] = u_day6_collect2view_ratio = (double)(d6_sum_act[1]  + 0.0001) / (d6_sum_act[0]  + 0.0001);
        d_f[101] = u_day6_collect2buy_ratio = (double)(d6_sum_act[1]  + 0.0001) / (d6_sum_act[3]  + 0.0001);       
        d_f[102] = u_day2_cart2view_items = (double)(set_day2_cart_items.size()  + 0.0001) / (set_day2_view_items.size()  + 0.0001);
        d_f[103] = u_day2_cart2buy_items = (double)(set_day2_cart_items.size()  + 0.0001) / (set_day2_buy_items.size()  + 0.0001);
        d_f[104] = u_day2_cart2view_days = (double)(set_d6_cart_days.size()  + 0.0001) / (set_d6_view_days.size()  + 0.0001);
        d_f[105] = u_day2_cart2buy_days = (double)(set_d6_cart_days.size()  + 0.0001) / (set_d6_buy_days.size()  + 0.0001);
        d_f[106] = u_day2_cart2view_ratio = (double)(d6_sum_act[2]  + 0.0001) / (d6_sum_act[0]  + 0.0001);
        d_f[107] = u_day2_cart2buy_ratio = (double)(d6_sum_act[2]  + 0.0001) / (d6_sum_act[3]  + 0.0001);
        d_f[108] = u_day2_cart2view_ratio_log_cart = u_day2_cart2view_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[109] = u_day2_cart2buy_ratio_log_cart = u_day2_cart2buy_ratio * Math.log(d6_sum_act[2] + 1.0);
        d_f[110] = u_day2_collect2view_ratio = (double)(d6_sum_act[1]  + 0.0001) / (d6_sum_act[0]  + 0.0001);
        d_f[111] = u_day2_collect2buy_ratio = (double)(d6_sum_act[1]  + 0.0001) / (d6_sum_act[3]  + 0.0001);
        result.set(0,key.get("uid"));
        for (int i = 1; i <= 40; i++)
        {
        	result.set(i,i_f[i-1]);
        }
        for (int j = 41; j <= 152; j++)
        {
        	result.set(j,d_f[j-41]);
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
    	if (dt_m == 12 && dt_d < 18 && dt_d >16)
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
    	//d3
    	if (dt_m == 12 && dt_d < 18 && dt_d >14)
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
    	//d7
    	if (dt_m == 12 && dt_d < 18 && dt_d >10)
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
    	//d14
    	if (dt_m == 12 && dt_d < 18 && dt_d >3)
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
    	if (dt_m == 12 && dt_d == 17)
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
    	//d2
    	if (dt_m == 12 && dt_d < 18 && dt_d >15)
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
    	//d3
    	if (dt_m == 12 && dt_d < 18 && dt_d >14)
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
    	//d6
    	if (dt_m == 12 && dt_d < 18 && dt_d >11)
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
    	//d7
    	if (dt_m == 12 && dt_d < 18 && dt_d >10)
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
    	//d13
    	if (dt_m == 12 && dt_d < 18 && dt_d >4)
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
    	//d14
    	if (dt_m == 12 && dt_d < 18 && dt_d >3)
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
    
    
}
