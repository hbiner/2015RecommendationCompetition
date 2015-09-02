package wonder.mapreduce.uc;

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
    
    private int offsize = 1;
    ///////////////////////////////////////////////////////
    //d1（14）
    private int uc_day1_view_count;	
    private int uc_day1_cart_count;	
    private int uc_day1_collect_count;		  	
    private int uc_day1_buy_count;	
    private int uc_day1_all_count;	
    private int uc_day1_is_cart;	
    private int uc_day1_is_buy;		
    private int uc_day1_is_cart_not_buy;		
    private int uc_day1_view_cross_hours;
    private int uc_day1_buy_cross_hours;
    private int uc_day1_behavior_cross_hours;
    private int uc_day1_cart_and_behavior_count;		
    private double uc_day1_collect_ratio;
    private double uc_day1_cart_ratio;
    private double uc_day1_buy_ratio;
    //d2（14）
    private int uc_day_2_view_count;			
    private int uc_day_2_cart_count;			
    private int uc_day_2_collect_count;		
    private int uc_day_2_buy_count;		
    private int uc_day_2_all_count;	
    private int uc_day_2_view_cross_hours;
    private int uc_day_2_buy_cross_hours;
    private int uc_day_2_behavior_cross_hours;		
    private int uc_day_2_cart_and_behavior_count;	
    private int uc_day_2_is_cart;			
    private int uc_day_2_is_buy;	
    private int uc_day_2_is_cart_not_buy;
    private double uc_day_2_collect_ratio;
    private double uc_day_2_cart_ratio;
    private double uc_day_2_buy_ratio;
    //d3（14）
    private int uc_day_3_view_count;			
    private int uc_day_3_cart_count;			
    private int uc_day_3_collect_count;		
    private int uc_day_3_buy_count;		
    private int uc_day_3_all_count;	
    private int uc_day_3_view_cross_hours;
    private int uc_day_3_buy_cross_hours;
    private int uc_day_3_behavior_cross_hours;		
    private int uc_day_3_cart_and_behavior_count;	
    private int uc_day_3_is_cart;			
    private int uc_day_3_is_buy;	
    private int uc_day_3_is_cart_not_buy;
    private double uc_day_3_collect_ratio;
    private double uc_day_3_cart_ratio;
    private double uc_day_3_buy_ratio;
    //d4（14）
    private int uc_day_4_view_count;			
    private int uc_day_4_cart_count;			
    private int uc_day_4_collect_count;		
    private int uc_day_4_buy_count;		
    private int uc_day_4_all_count;	
    private int uc_day_4_view_cross_hours;
    private int uc_day_4_buy_cross_hours;
    private int uc_day_4_behavior_cross_hours;		
    private int uc_day_4_cart_and_behavior_count;	
    private int uc_day_4_is_cart;			
    private int uc_day_4_is_buy;	
    private int uc_day_4_is_cart_not_buy;
    private double uc_day_4_collect_ratio;
    private double uc_day_4_cart_ratio;
    private double uc_day_4_buy_ratio;
    //d5（14）
    private int uc_day_5_view_count;			
    private int uc_day_5_cart_count;			
    private int uc_day_5_collect_count;		
    private int uc_day_5_buy_count;		
    private int uc_day_5_all_count;	
    private int uc_day_5_view_cross_hours;
    private int uc_day_5_buy_cross_hours;
    private int uc_day_5_behavior_cross_hours;		
    private int uc_day_5_cart_and_behavior_count;	
    private int uc_day_5_is_cart;			
    private int uc_day_5_is_buy;	
    private int uc_day_5_is_cart_not_buy;
    private double uc_day_5_collect_ratio;
    private double uc_day_5_cart_ratio;
    private double uc_day_5_buy_ratio;
    //d6（14）
    private int uc_day_6_view_count;			
    private int uc_day_6_cart_count;			
    private int uc_day_6_collect_count;		
    private int uc_day_6_buy_count;		
    private int uc_day_6_all_count;	
    private int uc_day_6_view_cross_hours;
    private int uc_day_6_buy_cross_hours;
    private int uc_day_6_behavior_cross_hours;		
    private int uc_day_6_cart_and_behavior_count;	
    private int uc_day_6_is_cart;			
    private int uc_day_6_is_buy;	
    private int uc_day_6_is_cart_not_buy;
    private double uc_day_6_collect_ratio;
    private double uc_day_6_cart_ratio;
    private double uc_day_6_buy_ratio;
    //d7（14）
    private int uc_day_7_view_count;			
    private int uc_day_7_cart_count;			
    private int uc_day_7_collect_count;		
    private int uc_day_7_buy_count;		
    private int uc_day_7_all_count;	
    private int uc_day_7_view_cross_hours;
    private int uc_day_7_buy_cross_hours;
    private int uc_day_7_behavior_cross_hours;		
    private int uc_day_7_cart_and_behavior_count;	
    private int uc_day_7_is_cart;			
    private int uc_day_7_is_buy;	
    private int uc_day_7_is_cart_not_buy;
    private double uc_day_7_collect_ratio;
    private double uc_day_7_cart_ratio;
    private double uc_day_7_buy_ratio;
    //d1-d3（14）
    private int uc_day_1_3_behavior_cross_hours;
    private int uc_day_1_3_view_cross_days;
    private int uc_day_1_3_buy_cross_days;
    private int uc_day_1_3_view_count;
    private int uc_day_1_3_collect_count;
    private int uc_day_1_3_cart_count;	
    private int uc_day_1_3_buy_count;
    private int uc_day_1_3_all_count;
    private int uc_day_1_3_is_cart;
    private int uc_day_1_3_is_buy;
    private int uc_day_1_3_is_cart_not_buy;
    private int uc_day_1_3_cart_and_behavior_count;
    private double uc_day_1_3_buy_ratio;
    private double uc_day_1_3_collect_ratio;	
    private double uc_day_1_3_cart_ratio;
    //d1-d7（14）
    private int uc_day_1_7_behavior_cross_hours;	
    private int uc_day_1_7_view_cross_days;
    private int uc_day_1_7_buy_cross_days;
    private int uc_day_1_7_view_count;
    private int uc_day_1_7_collect_count;
    private int uc_day_1_7_cart_count;	
    private int uc_day_1_7_buy_count;
    private int uc_day_1_7_all_count;
    private int uc_day_1_7_is_cart;
    private int uc_day_1_7_is_buy;
    private int uc_day_1_7_is_cart_not_buy;
    private int uc_day_1_7_cart_and_behavior_count;
    private double uc_day_1_7_buy_ratio;
    private double uc_day_1_7_collect_ratio;	
    private double uc_day_1_7_cart_ratio;
    //d1-d14（14）
    private int uc_day_1_14_behavior_cross_hours;	
    private int uc_day_1_14_view_cross_days;
    private int uc_day_1_14_buy_cross_days;
    private int uc_day_1_14_view_count;
    private int uc_day_1_14_collect_count;
    private int uc_day_1_14_cart_count;	
    private int uc_day_1_14_buy_count;
    private int uc_day_1_14_all_count;
    private int uc_day_1_14_is_cart;
    private int uc_day_1_14_is_buy;
    private int uc_day_1_14_is_cart_not_buy;
    private int uc_day_1_14_cart_and_behavior_count;
    private double uc_day_1_14_buy_ratio;
    private double uc_day_1_14_collect_ratio;	
    private double uc_day_1_14_cart_ratio;
    //d2-d3（13）
    private int uc_day_2_3_view_count;
    private int uc_day_2_3_cart_count;	
    private int uc_day_2_3_collect_count;	
    private int uc_day_2_3_buy_count;
    private int uc_day_2_3_all_count;
    private int uc_day_2_3_is_cart;
    private int uc_day_2_3_is_buy;	
    private int uc_day_2_3_is_cart_not_buy;	
    private int uc_day_2_3_behavior_cross_hours;
    private int uc_day_2_3_cart_and_behavior_count;
    private double uc_day_2_3_collect_ratio;
    private double uc_day_2_3_cart_ratio;	
    private double uc_day_2_3_buy_ratio;
    //d4-d7（13）
    private int uc_day_4_7_view_count;
    private int uc_day_4_7_cart_count;	
    private int uc_day_4_7_collect_count;	
    private int uc_day_4_7_buy_count;
    private int uc_day_4_7_all_count;
    private int uc_day_4_7_is_cart;
    private int uc_day_4_7_is_buy;	
    private int uc_day_4_7_is_cart_not_buy;	
    private int uc_day_4_7_behavior_cross_hours;
    private int uc_day_4_7_cart_and_behavior_count;
    private double uc_day_4_7_collect_ratio;
    private double uc_day_4_7_cart_ratio;	
    private double uc_day_4_7_buy_ratio;
    //d8-d14（13）
    private int uc_day_8_14_view_count;
    private int uc_day_8_14_cart_count;	
    private int uc_day_8_14_collect_count;	
    private int uc_day_8_14_buy_count;
    private int uc_day_8_14_all_count;
    private int uc_day_8_14_is_cart;
    private int uc_day_8_14_is_buy;	
    private int uc_day_8_14_is_cart_not_buy;	
    private int uc_day_8_14_behavior_cross_hours;
    private int uc_day_8_14_cart_and_behavior_count;	
    private double uc_day_8_14_collect_ratio;
    private double uc_day_8_14_cart_ratio;	
    private double uc_day_8_14_buy_ratio;
    //some ratio（8）
    private double uc_day1to2_3_cart_ratio;	
    private double uc_day_1_3to4_7_cart_ratio;	
    private double uc_day_1_7to8_14cart_ratio;
    private double uc_day1to2_change_ratio;	
    private double uc_day1to2_3_log_change_ratio;	
    private double uc_day1_2to3_5_log_change_ratio;	
    private double uc_day1_3to4_7_log_change_ratio;	
    private double uc_day1_7to8_14_log_change_ratio;
    //some connection（10）
    private int tuc_day2_continuous_buy;
    private int tuc_day3_continuous_buy;	
    private int tuc_pre_3_view_1_cart;	
    private int tuc_pre_7_view_1_cart;	
    private int tuc_pre_3_collect_1_cart;		
    private int tuc_pre_7_collect_1_cart;		
    private int tuc_pre_3_cart_1_cart;		
    private int tuc_pre_7_cart_1_cart;		
    private int tuc_pre_3_buy_1_cart;		
    private int tuc_pre_7_buy_1_cart;  
    private int uc_behavior_days;
    private int uc_behavior_hours;
    //////////////////////////////////////////////////////
    
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
    
    public void init()
    {
    	//d1（13）	
    	uc_day1_view_count = 0;	
    	uc_day1_cart_count = 0;	
    	uc_day1_collect_count = 0;		
    	uc_day1_buy_count = 0;	
    	uc_day1_all_count = 0;	
    	uc_day1_is_cart = 0;	
    	uc_day1_is_buy = 0;		
    	uc_day1_is_cart_not_buy = 0;
    	uc_day1_view_cross_hours = 0;
    	uc_day1_buy_cross_hours = 0;
    	uc_day1_behavior_cross_hours = 0;
    	uc_day1_cart_and_behavior_count = 0;		
    	uc_day1_collect_ratio = 0;
    	uc_day1_cart_ratio = 0;
    	uc_day1_buy_ratio = 0;
    	//d2（13）
    	uc_day_2_view_count = 0;			
    	uc_day_2_cart_count = 0;			
    	uc_day_2_collect_count = 0;		
    	uc_day_2_buy_count = 0;		
    	uc_day_2_all_count = 0;	
    	uc_day_2_view_cross_hours = 0;
    	uc_day_2_buy_cross_hours = 0;
    	uc_day_2_behavior_cross_hours = 0;		
    	uc_day_2_cart_and_behavior_count = 0;	
    	uc_day_2_is_cart = 0;			
    	uc_day_2_is_buy = 0;	
    	uc_day_2_is_cart_not_buy = 0;
    	uc_day_2_collect_ratio = 0;
    	uc_day_2_cart_ratio = 0;
    	uc_day_2_buy_ratio = 0;
    	//d3（13）
    	uc_day_3_view_count = 0;			
    	uc_day_3_cart_count = 0;			
    	uc_day_3_collect_count = 0;		
    	uc_day_3_buy_count = 0;		
    	uc_day_3_all_count = 0;	
    	uc_day_3_view_cross_hours = 0;
    	uc_day_3_buy_cross_hours = 0;
    	uc_day_3_behavior_cross_hours = 0;		
    	uc_day_3_cart_and_behavior_count = 0;	
    	uc_day_3_is_cart = 0;			
    	uc_day_3_is_buy = 0;	
    	uc_day_3_is_cart_not_buy = 0;
    	uc_day_3_collect_ratio = 0;
    	uc_day_3_cart_ratio = 0;
    	uc_day_3_buy_ratio = 0;
    	//d4（13）
    	uc_day_4_view_count = 0;			
    	uc_day_4_cart_count = 0;			
    	uc_day_4_collect_count = 0;		
    	uc_day_4_buy_count = 0;		
    	uc_day_4_all_count = 0;	
    	uc_day_4_view_cross_hours = 0;
    	uc_day_4_buy_cross_hours = 0;
    	uc_day_4_behavior_cross_hours = 0;		
    	uc_day_4_cart_and_behavior_count = 0;	
    	uc_day_4_is_cart = 0;			
    	uc_day_4_is_buy = 0;	
    	uc_day_4_is_cart_not_buy = 0;
    	uc_day_4_collect_ratio = 0;
    	uc_day_4_cart_ratio = 0;
    	uc_day_4_buy_ratio = 0;
    	//d5（13）
    	uc_day_5_view_count = 0;			
    	uc_day_5_cart_count = 0;			
    	uc_day_5_collect_count = 0;		
    	uc_day_5_buy_count = 0;		
    	uc_day_5_all_count = 0;	
    	uc_day_5_view_cross_hours = 0;
    	uc_day_5_buy_cross_hours = 0;
    	uc_day_5_behavior_cross_hours = 0;		
    	uc_day_5_cart_and_behavior_count = 0;	
    	uc_day_5_is_cart = 0;			
    	uc_day_5_is_buy = 0;	
    	uc_day_5_is_cart_not_buy = 0;
    	uc_day_5_collect_ratio = 0;
    	uc_day_5_cart_ratio = 0;
    	uc_day_5_buy_ratio = 0;
    	//d6（13）
    	uc_day_6_view_count = 0;			
    	uc_day_6_cart_count = 0;			
    	uc_day_6_collect_count = 0;		
    	uc_day_6_buy_count = 0;		
    	uc_day_6_all_count = 0;	
    	uc_day_6_view_cross_hours = 0;
    	uc_day_6_buy_cross_hours = 0;
    	uc_day_6_behavior_cross_hours = 0;		
    	uc_day_6_cart_and_behavior_count = 0;	
    	uc_day_6_is_cart = 0;			
    	uc_day_6_is_buy = 0;	
    	uc_day_6_is_cart_not_buy = 0;
    	uc_day_6_collect_ratio = 0;
    	uc_day_6_cart_ratio = 0;
    	uc_day_6_buy_ratio = 0;
    	//d7（13）
    	uc_day_7_view_count = 0;			
    	uc_day_7_cart_count = 0;			
    	uc_day_7_collect_count = 0;		
    	uc_day_7_buy_count = 0;		
    	uc_day_7_all_count = 0;	
    	uc_day_7_view_cross_hours = 0;
    	uc_day_7_buy_cross_hours = 0;
    	uc_day_7_behavior_cross_hours = 0;		
    	uc_day_7_cart_and_behavior_count = 0;	
    	uc_day_7_is_cart = 0;			
    	uc_day_7_is_buy = 0;	
    	uc_day_7_is_cart_not_buy = 0;
    	uc_day_7_collect_ratio = 0;
    	uc_day_7_cart_ratio = 0;
    	uc_day_7_buy_ratio = 0;
    	//d1-d3（14）
    	uc_day_1_3_behavior_cross_hours = 0;	
    	uc_day_1_3_view_cross_days = 0;
    	uc_day_1_3_buy_cross_days = 0;
    	uc_day_1_3_view_count = 0;
    	uc_day_1_3_collect_count = 0;
    	uc_day_1_3_cart_count = 0;	
    	uc_day_1_3_buy_count = 0;
    	uc_day_1_3_all_count = 0;
    	uc_day_1_3_is_cart = 0;
    	uc_day_1_3_is_buy = 0;
    	uc_day_1_3_is_cart_not_buy = 0;
    	uc_day_1_3_cart_and_behavior_count = 0;
    	uc_day_1_3_buy_ratio = 0;
    	uc_day_1_3_collect_ratio = 0;	
    	uc_day_1_3_cart_ratio = 0;
    	//d1-d7（14）
    	uc_day_1_7_behavior_cross_hours = 0;	
    	uc_day_1_7_view_cross_days = 0;
    	uc_day_1_7_buy_cross_days = 0;
    	uc_day_1_7_view_count = 0;
    	uc_day_1_7_collect_count = 0;
    	uc_day_1_7_cart_count = 0;	
    	uc_day_1_7_buy_count = 0;
    	uc_day_1_7_all_count = 0;
    	uc_day_1_7_is_cart = 0;
    	uc_day_1_7_is_buy = 0;
    	uc_day_1_7_is_cart_not_buy = 0;
    	uc_day_1_7_cart_and_behavior_count = 0;
    	uc_day_1_7_buy_ratio = 0;
    	uc_day_1_7_collect_ratio = 0;	
    	uc_day_1_7_cart_ratio = 0;
    	//d1-d14（14）
    	uc_day_1_14_behavior_cross_hours = 0;	
    	uc_day_1_14_view_cross_days = 0;
    	uc_day_1_14_buy_cross_days = 0;
    	uc_day_1_14_view_count = 0;
    	uc_day_1_14_collect_count = 0;
    	uc_day_1_14_cart_count = 0;	
    	uc_day_1_14_buy_count = 0;
    	uc_day_1_14_all_count = 0;
    	uc_day_1_14_is_cart = 0;
    	uc_day_1_14_is_buy = 0;
    	uc_day_1_14_is_cart_not_buy = 0;
    	uc_day_1_14_cart_and_behavior_count = 0;
    	uc_day_1_14_buy_ratio = 0;
    	uc_day_1_14_collect_ratio = 0;	
    	uc_day_1_14_cart_ratio = 0;
    	//d2-d3(13)
    	uc_day_2_3_view_count = 0;
    	uc_day_2_3_cart_count = 0;	
    	uc_day_2_3_collect_count = 0;	
    	uc_day_2_3_buy_count = 0;
    	uc_day_2_3_all_count = 0;
    	uc_day_2_3_is_cart = 0;
    	uc_day_2_3_is_buy = 0;	
    	uc_day_2_3_is_cart_not_buy = 0;	
    	uc_day_2_3_behavior_cross_hours = 0;
    	uc_day_2_3_cart_and_behavior_count = 0;
    	uc_day_2_3_collect_ratio = 0;
    	uc_day_2_3_cart_ratio = 0;	
    	uc_day_2_3_buy_ratio = 0;
    	//d4-d7(13)
    	uc_day_4_7_view_count = 0;
    	uc_day_4_7_cart_count = 0;	
    	uc_day_4_7_collect_count = 0;	
    	uc_day_4_7_buy_count = 0;
    	uc_day_4_7_all_count = 0;
    	uc_day_4_7_is_cart = 0;
    	uc_day_4_7_is_buy = 0;	
    	uc_day_4_7_is_cart_not_buy = 0;	
    	uc_day_4_7_behavior_cross_hours = 0;
    	uc_day_4_7_cart_and_behavior_count = 0;
    	uc_day_4_7_collect_ratio = 0;
    	uc_day_4_7_cart_ratio = 0;	
    	uc_day_4_7_buy_ratio = 0;
    	//d8-d14(13)
    	uc_day_8_14_view_count = 0;
    	uc_day_8_14_cart_count = 0;	
    	uc_day_8_14_collect_count = 0;	
    	uc_day_8_14_buy_count = 0;
    	uc_day_8_14_all_count = 0;
    	uc_day_8_14_is_cart = 0;
    	uc_day_8_14_is_buy = 0;	
    	uc_day_8_14_is_cart_not_buy = 0;	
    	uc_day_8_14_behavior_cross_hours = 0;
    	uc_day_8_14_cart_and_behavior_count = 0;		
    	uc_day_8_14_collect_ratio = 0;
    	uc_day_8_14_cart_ratio = 0;	
    	uc_day_8_14_buy_ratio = 0;
    	//some ratio
    	uc_day1to2_3_cart_ratio = 0;	
    	uc_day_1_3to4_7_cart_ratio = 0;	
    	uc_day_1_7to8_14cart_ratio = 0;
    	uc_day1to2_change_ratio = 0;	
    	uc_day1to2_3_log_change_ratio = 0;	
    	uc_day1_2to3_5_log_change_ratio = 0;	
    	uc_day1_3to4_7_log_change_ratio = 0;	
    	uc_day1_7to8_14_log_change_ratio = 0;
    	//some connection
    	tuc_day2_continuous_buy = 0;
    	tuc_day3_continuous_buy = 0;	
    	tuc_pre_3_view_1_cart = 0;	
    	tuc_pre_7_view_1_cart = 0;	
    	tuc_pre_3_collect_1_cart = 0;		
    	tuc_pre_7_collect_1_cart = 0;		
    	tuc_pre_3_cart_1_cart = 0;		
    	tuc_pre_7_cart_1_cart = 0;		
    	tuc_pre_3_buy_1_cart = 0;		
    	tuc_pre_7_buy_1_cart = 0;
    	uc_behavior_days = 0;
    	uc_behavior_hours = 0;
        
        for (int i = 0; i < 14; i++)
        {
        	uc_day_bahavior_max_hour[i] = -1;
            uc_day_bahavior_min_hour[i] = -1;
            uc_day_bahavior_max_dt[i] = -1;
            uc_day_bahavior_min_dt[i] = -1;
            uc_day_view_max_hour[i] = -1;
            uc_day_view_min_hour[i] = -1;
            uc_day_view_max_dt[i] = -1;
            uc_day_view_min_dt[i] = -1;
            uc_day_buy_max_hour[i] = -1;
            uc_day_buy_min_hour[i] = -1;
            uc_day_buy_max_dt[i] = -1;
            uc_day_buy_min_dt[i] = -1;    
            uc_day_view_count[i] = 0;
            uc_day_collect_count[i] = 0;
            uc_day_cart_count[i] = 0;
            uc_day_buy_count[i] = 0;
            uc_day_behavior_count[i] = 0;
        }
        set_uc_behavior_days.clear();
        set_uc_behavior_hours.clear();
    }
    //////////////////////////////////////////////////1   2   3   4   5   6   7 1-3  1-7 1-14 8-14 all 2-3 4-7
    private static int uc_day_bahavior_max_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1, -1,  -1, -1, -1,  -1, -1};//1/2/3/4/5/6/7/1-3/1-7/1-14/2-3/4-7/8-14/all;
    private static int uc_day_bahavior_min_hour[] = {-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_bahavior_max_dt[] = 	{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_bahavior_min_dt[] = 	{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_view_max_hour[] = 	{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};//1/2/3/4/5/6/7/1-3/1-7/1-14/2-3/4-7/8-14/all;
    private static int uc_day_view_min_hour[] = 	{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_view_max_dt[] = 		{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_view_min_dt[] = 		{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_buy_max_hour[] = 		{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};//1/2/3/4/5/6/7/1-3/1-7/1-14/2-3/4-7/8-14/all;
    private static int uc_day_buy_min_hour[] = 		{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_buy_max_dt[] = 		{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
    private static int uc_day_buy_min_dt[] = 		{-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};    
    private static int uc_day_view_count[] = 		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};//1/2/3/4/5/6/7/1-3/1-7/1-14/2-3/4-7/8-14/all;
    private static int uc_day_collect_count[] = 	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static int uc_day_cart_count[] = 		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static int uc_day_buy_count[] = 		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static int uc_day_behavior_count[] = 	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private Set set_uc_behavior_days = new HashSet();
    private Set set_uc_behavior_hours = new HashSet();

   
    //d1/d2/d3/d4/d5/d6/d7 
    public void genFeature(int tp, int hr, int dt_m, int dt_d)
    {
    	int dis_hr = (17 + offsize - dt_d) * 24 + (24 - hr);
    	int dis_dt_d = 18 + offsize - dt_d;
    	//d1
    	if (dt_m == 12 && dt_d > 16 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[0] == -1)
    		{
    			uc_day_bahavior_max_hour[0] = uc_day_bahavior_min_hour[0] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[0] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[0] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[0] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[0] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[0] == -1)
        		{
        			uc_day_view_max_hour[0] = uc_day_view_min_hour[0] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[0] < dis_hr)
        			{
        				uc_day_view_max_hour[0] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[0] > dis_hr)
        			{
        				uc_day_view_min_hour[0] = dis_hr;
        			}
        		}
    			uc_day_view_count[0] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[0] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[0] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[0] == -1)
        		{
        			uc_day_buy_max_hour[0] = uc_day_buy_min_hour[0] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[0] < dis_hr)
        			{
        				uc_day_buy_max_hour[0] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[0] > dis_hr)
        			{
        				uc_day_buy_min_hour[0] = dis_hr;
        			}
        		}
    			uc_day_buy_count[0] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[0] += 1;
    	}
    	//d2
    	if (dt_m == 12 && dt_d > 15 + offsize && dt_d < 17 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[1] == -1)
    		{
    			uc_day_bahavior_max_hour[1] = uc_day_bahavior_min_hour[1] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[1] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[1] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[1] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[1] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[1] == -1)
        		{
        			uc_day_view_max_hour[1] = uc_day_view_min_hour[1] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[1] < dis_hr)
        			{
        				uc_day_view_max_hour[1] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[1] > dis_hr)
        			{
        				uc_day_view_min_hour[1] = dis_hr;
        			}
        		}
    			uc_day_view_count[1] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[1] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[1] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[1] == -1)
        		{
        			uc_day_buy_max_hour[1] = uc_day_buy_min_hour[1] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[1] < dis_hr)
        			{
        				uc_day_buy_max_hour[1] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[1] > dis_hr)
        			{
        				uc_day_buy_min_hour[1] = dis_hr;
        			}
        		}
    			uc_day_buy_count[1] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[1] += 1;
    	}
    	//d3
    	if (dt_m == 12 && dt_d > 14 + offsize && dt_d < 16 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[2] == -1)
    		{
    			uc_day_bahavior_max_hour[2] = uc_day_bahavior_min_hour[2] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[2] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[2] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[2] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[2] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[2] == -1)
        		{
        			uc_day_view_max_hour[2] = uc_day_view_min_hour[2] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[2] < dis_hr)
        			{
        				uc_day_view_max_hour[2] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[2] > dis_hr)
        			{
        				uc_day_view_min_hour[2] = dis_hr;
        			}
        		}
    			uc_day_view_count[2] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[2] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[2] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[2] == -1)
        		{
        			uc_day_buy_max_hour[2] = uc_day_buy_min_hour[2] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[2] < dis_hr)
        			{
        				uc_day_buy_max_hour[2] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[2] > dis_hr)
        			{
        				uc_day_buy_min_hour[2] = dis_hr;
        			}
        		}
    			uc_day_buy_count[2] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[2] += 1;
    	}
    	//d4
    	if (dt_m == 12 && dt_d > 13 + offsize && dt_d < 15 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[3] == -1)
    		{
    			uc_day_bahavior_max_hour[3] = uc_day_bahavior_min_hour[3] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[3] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[3] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[3] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[3] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[3] == -1)
        		{
        			uc_day_view_max_hour[3] = uc_day_view_min_hour[3] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[3] < dis_hr)
        			{
        				uc_day_view_max_hour[3] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[3] > dis_hr)
        			{
        				uc_day_view_min_hour[3] = dis_hr;
        			}
        		}
    			uc_day_view_count[3] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[3] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[3] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[3] == -1)
        		{
        			uc_day_buy_max_hour[3] = uc_day_buy_min_hour[3] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[3] < dis_hr)
        			{
        				uc_day_buy_max_hour[3] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[3] > dis_hr)
        			{
        				uc_day_buy_min_hour[3] = dis_hr;
        			}
        		}
    			uc_day_buy_count[3] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[3] += 1;
    	}
    	//d5
    	if (dt_m == 12 && dt_d > 12 + offsize && dt_d < 14 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[4] == -1)
    		{
    			uc_day_bahavior_max_hour[4] = uc_day_bahavior_min_hour[4] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[4] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[4] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[4] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[4] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[4] == -1)
        		{
        			uc_day_view_max_hour[4] = uc_day_view_min_hour[4] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[4] < dis_hr)
        			{
        				uc_day_view_max_hour[4] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[4] > dis_hr)
        			{
        				uc_day_view_min_hour[4] = dis_hr;
        			}
        		}
    			uc_day_view_count[4] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[4] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[4] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[4] == -1)
        		{
        			uc_day_buy_max_hour[4] = uc_day_buy_min_hour[4] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[4] < dis_hr)
        			{
        				uc_day_buy_max_hour[4] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[4] > dis_hr)
        			{
        				uc_day_buy_min_hour[4] = dis_hr;
        			}
        		}
    			uc_day_buy_count[4] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[4] += 1;
    	}
    	//d6
    	if (dt_m == 12 && dt_d > 11 + offsize && dt_d < 13 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[5] == -1)
    		{
    			uc_day_bahavior_max_hour[5] = uc_day_bahavior_min_hour[5] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[5] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[5] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[5] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[5] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[5] == -1)
        		{
        			uc_day_view_max_hour[5] = uc_day_view_min_hour[5] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[5] < dis_hr)
        			{
        				uc_day_view_max_hour[5] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[5] > dis_hr)
        			{
        				uc_day_view_min_hour[5] = dis_hr;
        			}
        		}
    			uc_day_view_count[5] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[5] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[5] += 1;
    			break;
    		case 5:
    			if (uc_day_buy_max_hour[5] == -1)
        		{
        			uc_day_buy_max_hour[5] = uc_day_buy_min_hour[5] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[5] < dis_hr)
        			{
        				uc_day_buy_max_hour[5] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[5] > dis_hr)
        			{
        				uc_day_buy_min_hour[5] = dis_hr;
        			}
        		}
    			uc_day_buy_count[5] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[5] += 1;
    	}
    	//d7
    	if (dt_m == 12 && dt_d > 10 + offsize && dt_d < 12 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[6] == -1)
    		{
    			uc_day_bahavior_max_hour[6] = uc_day_bahavior_min_hour[6] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[6] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[6] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[6] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[6] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[6] == -1)
        		{
        			uc_day_view_max_hour[6] = uc_day_view_min_hour[6] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[6] < dis_hr)
        			{
        				uc_day_view_max_hour[6] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[6] > dis_hr)
        			{
        				uc_day_view_min_hour[6] = dis_hr;
        			}
        		}
    			uc_day_view_count[6] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[6] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[6] += 1;
    			break;
    		case 6:
    			if (uc_day_buy_max_hour[6] == -1)
        		{
        			uc_day_buy_max_hour[6] = uc_day_buy_min_hour[6] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[6] < dis_hr)
        			{
        				uc_day_buy_max_hour[6] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[6] > dis_hr)
        			{
        				uc_day_buy_min_hour[6] = dis_hr;
        			}
        		}
    			uc_day_buy_count[6] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[6] += 1;
    	}

    }
   
    //d1-d3/d1-d7/d1-d14
    public void genFeature1(int tp, int hr, int dt_m, int dt_d)
    {
    	int dis_hr = (17 + offsize - dt_d) * 24 + (24 - hr);
    	int dis_dt_d = 18 + offsize - dt_d;
    	//d1-d3
    	if (dt_m == 12 && dt_d > 14 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[7] == -1)
    		{
    			uc_day_bahavior_max_hour[7] = uc_day_bahavior_min_hour[7] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[7] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[7] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[7] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[7] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_bahavior_max_dt[7] == -1)
    		{
    			uc_day_bahavior_max_dt[7] = uc_day_bahavior_min_dt[7] = dt_d;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_dt[7] < dt_d)
    			{
    				uc_day_bahavior_max_dt[7] = dt_d;
    			}
    			else if (uc_day_bahavior_min_dt[7] > dt_d)
    			{
    				uc_day_bahavior_min_dt[7] = dt_d;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[7] == -1)
        		{
        			uc_day_view_max_hour[7] = uc_day_view_min_hour[7] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[7] < dis_hr)
        			{
        				uc_day_view_max_hour[7] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[7] > dis_hr)
        			{
        				uc_day_view_min_hour[7] = dis_hr;
        			}
        		}
    			//date
        		if (uc_day_view_max_dt[7] == -1)
        		{
        			uc_day_view_max_dt[7] = uc_day_view_min_dt[7] = dt_d;
        		}
        		else 
        		{
        			if (uc_day_view_max_dt[7] < dt_d)
        			{
        				uc_day_view_max_dt[7] = dt_d;
        			}
        			else if (uc_day_view_min_dt[7] > dt_d)
        			{
        				uc_day_view_min_dt[7] = dt_d;
        			}
        		}
    			uc_day_view_count[7] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[7] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[7] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[7] == -1)
        		{
        			uc_day_buy_max_hour[7] = uc_day_buy_min_hour[7] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[7] < dis_hr)
        			{
        				uc_day_buy_max_hour[7] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[7] > dis_hr)
        			{
        				uc_day_buy_min_hour[7] = dis_hr;
        			}
        		}
    			//date
        		if (uc_day_buy_max_dt[7] == -1)
        		{
        			uc_day_buy_max_dt[7] = uc_day_buy_min_dt[7] = dt_d;
        		}
        		else 
        		{
        			if (uc_day_buy_max_dt[7] < dt_d)
        			{
        				uc_day_buy_max_dt[7] = dt_d;
        			}
        			else if (uc_day_buy_min_dt[7] > dt_d)
        			{
        				uc_day_buy_min_dt[7] = dt_d;
        			}
        		}
    			uc_day_buy_count[7] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[7] += 1;
    	}
    	//d1-d7
    	if (dt_m == 12 && dt_d > 10 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[8] == -1)
    		{
    			uc_day_bahavior_max_hour[8] = uc_day_bahavior_min_hour[8] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[8] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[8] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[8] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[8] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_bahavior_max_dt[8] == -1)
    		{
    			uc_day_bahavior_max_dt[8] = uc_day_bahavior_min_dt[8] = dt_d;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_dt[8] < dt_d)
    			{
    				uc_day_bahavior_max_dt[8] = dt_d;
    			}
    			else if (uc_day_bahavior_min_dt[8] > dt_d)
    			{
    				uc_day_bahavior_min_dt[8] = dt_d;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[8] == -1)
        		{
        			uc_day_view_max_hour[8] = uc_day_view_min_hour[8] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[8] < dis_hr)
        			{
        				uc_day_view_max_hour[8] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[8] > dis_hr)
        			{
        				uc_day_view_min_hour[8] = dis_hr;
        			}
        		}
    			//date
        		if (uc_day_view_max_dt[8] == -1)
        		{
        			uc_day_view_max_dt[8] = uc_day_view_min_dt[8] = dt_d;
        		}
        		else 
        		{
        			if (uc_day_view_max_dt[8] < dt_d)
        			{
        				uc_day_view_max_dt[8] = dt_d;
        			}
        			else if (uc_day_view_min_dt[8] > dt_d)
        			{
        				uc_day_view_min_dt[8] = dt_d;
        			}
        		}
    			uc_day_view_count[8] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[8] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[8] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[8] == -1)
        		{
        			uc_day_buy_max_hour[8] = uc_day_buy_min_hour[8] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[8] < dis_hr)
        			{
        				uc_day_buy_max_hour[8] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[8] > dis_hr)
        			{
        				uc_day_buy_min_hour[8] = dis_hr;
        			}
        		}
    			//date
        		if (uc_day_buy_max_dt[8] == -1)
        		{
        			uc_day_buy_max_dt[8] = uc_day_buy_min_dt[8] = dt_d;
        		}
        		else 
        		{
        			if (uc_day_buy_max_dt[8] < dt_d)
        			{
        				uc_day_buy_max_dt[8] = dt_d;
        			}
        			else if (uc_day_buy_min_dt[8] > dt_d)
        			{
        				uc_day_buy_min_dt[8] = dt_d;
        			}
        		}
    			uc_day_buy_count[8] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[8] += 1;
    	}
    	//d1-d14
    	if (dt_m == 12 && dt_d > 3 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[9] == -1)
    		{
    			uc_day_bahavior_max_hour[9] = uc_day_bahavior_min_hour[9] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[9] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[9] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[9] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[9] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_bahavior_max_dt[9] == -1)
    		{
    			uc_day_bahavior_max_dt[9] = uc_day_bahavior_min_dt[9] = dt_d;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_dt[9] < dt_d)
    			{
    				uc_day_bahavior_max_dt[9] = dt_d;
    			}
    			else if (uc_day_bahavior_min_dt[9] > dt_d)
    			{
    				uc_day_bahavior_min_dt[9] = dt_d;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (uc_day_view_max_hour[9] == -1)
        		{
        			uc_day_view_max_hour[9] = uc_day_view_min_hour[9] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_view_max_hour[9] < dis_hr)
        			{
        				uc_day_view_max_hour[9] = dis_hr;
        			}
        			else if (uc_day_view_min_hour[9] > dis_hr)
        			{
        				uc_day_view_min_hour[9] = dis_hr;
        			}
        		}
    			//date
        		if (uc_day_view_max_dt[9] == -1)
        		{
        			uc_day_view_max_dt[9] = uc_day_view_min_dt[9] = dt_d;
        		}
        		else 
        		{
        			if (uc_day_view_max_dt[9] < dt_d)
        			{
        				uc_day_view_max_dt[9] = dt_d;
        			}
        			else if (uc_day_view_min_dt[9] > dt_d)
        			{
        				uc_day_view_min_dt[9] = dt_d;
        			}
        		}
    			uc_day_view_count[9] += 1;
    			break;
    		case 2:
    			uc_day_collect_count[9] += 1;
    			break;
    		case 3:
    			uc_day_cart_count[9] += 1;
    			break;
    		case 4:
    			if (uc_day_buy_max_hour[9] == -1)
        		{
        			uc_day_buy_max_hour[9] = uc_day_buy_min_hour[9] = dis_hr;
        		}
        		else 
        		{
        			if (uc_day_buy_max_hour[9] < dis_hr)
        			{
        				uc_day_buy_max_hour[9] = dis_hr;
        			}
        			else if (uc_day_buy_min_hour[9] > dis_hr)
        			{
        				uc_day_buy_min_hour[9] = dis_hr;
        			}
        		}
    			//date
        		if (uc_day_buy_max_dt[9] == -1)
        		{
        			uc_day_buy_max_dt[9] = uc_day_buy_min_dt[9] = dt_d;
        		}
        		else 
        		{
        			if (uc_day_buy_max_dt[9] < dt_d)
        			{
        				uc_day_buy_max_dt[9] = dt_d;
        			}
        			else if (uc_day_buy_min_dt[9] > dt_d)
        			{
        				uc_day_buy_min_dt[9] = dt_d;
        			}
        		}
    			uc_day_buy_count[9] += 1;
    			break;
    		default:
    			break;
    		}
    		uc_day_behavior_count[9] += 1;
    	}
    }
   
    //all
    public void genFeature2(int tp, int hr, int dt_m, int dt_d)
    {
    	int dis_hr = (17 + offsize - dt_d) * 24 + (24 - hr);
    	int dis_dt_d;
    	if (dt_m == 12)
    	{
    		dis_dt_d = 18 + offsize - dt_d;
    	}
    	else
    	{
    		dis_dt_d = 18 + offsize + 30 - dt_d;
    	}
    	
    	set_uc_behavior_days.add(dis_dt_d);
    	set_uc_behavior_hours.add(dis_hr);
    	
    	//hour
    	if (uc_day_bahavior_max_hour[11] == -1)
    	{
    		uc_day_bahavior_max_hour[11] = uc_day_bahavior_min_hour[11] = dis_hr;
//    		uc_last_behavior_type = tp;
    	}
    	else 
    	{
    		if (uc_day_bahavior_max_hour[11] < dis_hr)
    		{
    			uc_day_bahavior_max_hour[11] = dis_hr;
    		}
    		else if (uc_day_bahavior_min_hour[11] > dis_hr)
    		{
    			uc_day_bahavior_min_hour[11] = dis_hr;
//    			uc_last_behavior_type = tp;
    		}
    	}
    	//date
    	if (uc_day_bahavior_max_dt[11] == -1)
    	{
    		uc_day_bahavior_max_dt[11] = uc_day_bahavior_min_dt[11] = dis_dt_d;
    	}
    	else 
    	{
    		if (uc_day_bahavior_max_dt[11] < dis_dt_d)
    		{
    			uc_day_bahavior_max_dt[11] = dis_dt_d;
    		}
    		else if (uc_day_bahavior_min_dt[11] > dis_dt_d)
    		{
    			uc_day_bahavior_min_dt[11] = dis_dt_d;
    		}
    	}
    	switch(tp)
    	{
    	case 1:
    		if (uc_day_view_max_hour[11] == -1)
    		{
    			uc_day_view_max_hour[11] = uc_day_view_min_hour[11] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_view_max_hour[11] < dis_hr)
    			{
    				uc_day_view_max_hour[11] = dis_hr;
    			}
    			else if (uc_day_view_min_hour[11] > dis_hr)
    			{
    				uc_day_view_min_hour[11] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_view_max_dt[11] == -1)
    		{
    			uc_day_view_max_dt[11] = uc_day_view_min_dt[11] = dis_dt_d;
    		}
    		else 
    		{
    			if (uc_day_view_max_dt[11] < dis_dt_d)
    			{
    				uc_day_view_max_dt[11] = dis_dt_d;
    			}
    			else if (uc_day_view_min_dt[11] > dis_dt_d)
    			{
    				uc_day_view_min_dt[11] = dis_dt_d;
    			}
    		}
    		uc_day_view_count[11] += 1;
    		break;
    	case 2:
    		uc_day_collect_count[11] += 1;
    		break;
    	case 3:
    		uc_day_cart_count[11] += 1;
    		break;
    	case 4:
    		if (uc_day_buy_max_hour[11] == -1)
    		{
    			uc_day_buy_max_hour[11] = uc_day_buy_min_hour[11] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_buy_max_hour[11] < dis_hr)
    			{
    				uc_day_buy_max_hour[11] = dis_hr;
    			}
    			else if (uc_day_buy_min_hour[11] > dis_hr)
    			{
    				uc_day_buy_min_hour[11] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_buy_max_dt[11] == -1)
    		{
    			uc_day_buy_max_dt[11] = uc_day_buy_min_dt[11] = dis_dt_d;
    		}
    		else 
    		{
    			if (uc_day_buy_max_dt[11] < dis_dt_d)
    			{
    				uc_day_buy_max_dt[11] = dis_dt_d;
    			}
    			else if (uc_day_buy_min_dt[11] > dis_dt_d)
    			{
    				uc_day_buy_min_dt[11] = dis_dt_d;
    			}
    		}
    		uc_day_buy_count[11] += 1;
    		break;
    	default:
    		break;
    	}
    	uc_day_behavior_count[11] += 1;
    	
    	//d8-d14
    	if (dt_m == 12 && dt_d > 3 + offsize && dt_d < 11 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[10] == -1)
    		{
    			uc_day_bahavior_max_hour[10] = uc_day_bahavior_min_hour[10] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[10] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[10] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[10] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[10] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_bahavior_max_dt[10] == -1)
    		{
    			uc_day_bahavior_max_dt[10] = uc_day_bahavior_min_dt[10] = dis_dt_d;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_dt[10] < dis_dt_d)
    			{
    				uc_day_bahavior_max_dt[10] = dis_dt_d;
    			}
    			else if (uc_day_bahavior_min_dt[10] > dis_dt_d)
    			{
    				uc_day_bahavior_min_dt[10] = dis_dt_d;
    			}
    		}
    	}
    	//d2-d3
    	if (dt_m == 12 && dt_d > 14 + offsize && dt_d < 17 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[12] == -1)
    		{
    			uc_day_bahavior_max_hour[12] = uc_day_bahavior_min_hour[12] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[12] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[12] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[12] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[12] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_bahavior_max_dt[12] == -1)
    		{
    			uc_day_bahavior_max_dt[12] = uc_day_bahavior_min_dt[12] = dis_dt_d;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_dt[12] < dis_dt_d)
    			{
    				uc_day_bahavior_max_dt[12] = dis_dt_d;
    			}
    			else if (uc_day_bahavior_min_dt[12] > dis_dt_d)
    			{
    				uc_day_bahavior_min_dt[12] = dis_dt_d;
    			}
    		}
    	}
    	//d4-d7
    	if (dt_m == 12 && dt_d > 10 + offsize && dt_d < 15 + offsize)
    	{
    		//hour
    		if (uc_day_bahavior_max_hour[13] == -1)
    		{
    			uc_day_bahavior_max_hour[13] = uc_day_bahavior_min_hour[13] = dis_hr;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_hour[13] < dis_hr)
    			{
    				uc_day_bahavior_max_hour[13] = dis_hr;
    			}
    			else if (uc_day_bahavior_min_hour[13] > dis_hr)
    			{
    				uc_day_bahavior_min_hour[13] = dis_hr;
    			}
    		}
    		//date
    		if (uc_day_bahavior_max_dt[13] == -1)
    		{
    			uc_day_bahavior_max_dt[13] = uc_day_bahavior_min_dt[13] = dis_dt_d;
    		}
    		else 
    		{
    			if (uc_day_bahavior_max_dt[13] < dis_dt_d)
    			{
    				uc_day_bahavior_max_dt[13] = dis_dt_d;
    			}
    			else if (uc_day_bahavior_min_dt[13] > dis_dt_d)
    			{
    				uc_day_bahavior_min_dt[13] = dis_dt_d;
    			}
    		}
    	}
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
            int tp = new Long(val.getBigint("tp")).intValue();
            String dt = val.getString("dt");
            int hr = new Long(val.getBigint("hr")).intValue();
            String[] parts = dt.split("-");
            
            // by lemon 
            String dt_YMD ="";
            String iid = val.getString("iid");
            String ug = val.getString("ug");
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
            
            
            
            
            genFeature(tp, hr, dt_m, dt_d);//0-83
            genFeature1(tp, hr, dt_m, dt_d);//84-
            genFeature2(tp, hr, dt_m, dt_d);//132,133
            
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
            lm_makingFeatures(iid, ug, tp, (int)timeDistance, hr);
    		
        }
        int i_f[] = new int[200];
        double d_f[] = new double[100];
        //d1
        i_f[0] = uc_day1_behavior_cross_hours = (uc_day_bahavior_max_hour[0] == -1) ? 0 : uc_day_bahavior_max_hour[0] - uc_day_bahavior_min_hour[0] + 1;   
        i_f[1] = uc_day1_view_cross_hours = (uc_day_view_max_hour[0] == -1) ? 0 : uc_day_view_max_hour[0] - uc_day_view_min_hour[0] + 1; 
        i_f[2] = uc_day1_buy_cross_hours = (uc_day_buy_max_hour[0] == -1) ? 0 : uc_day_buy_max_hour[0] - uc_day_buy_min_hour[0] + 1;
        i_f[3] = uc_day1_view_count = uc_day_view_count[0];
        i_f[4] = uc_day1_collect_count = uc_day_collect_count[0];
        i_f[5] = uc_day1_cart_count = uc_day_cart_count[0];
        i_f[6] = uc_day1_buy_count = uc_day_buy_count[0];
        i_f[7] = uc_day1_all_count = uc_day_behavior_count[0];
        i_f[8] = uc_day1_cart_and_behavior_count = uc_day1_cart_count * uc_day1_all_count;//??????????????
        i_f[9] = uc_day1_is_cart = (uc_day1_cart_count > 0) ? 1:0;
        i_f[10] = uc_day1_is_buy = (uc_day1_buy_count > 0) ? 1:0;
        i_f[11] = uc_day1_is_cart_not_buy = (uc_day1_is_cart == 1 && uc_day1_is_buy == 0) ? 1:0;
        d_f[0] = uc_day1_collect_ratio = (double)(uc_day1_collect_count + 0.0001)/(uc_day1_view_count+uc_day1_collect_count+uc_day1_cart_count+uc_day1_buy_count + 0.0001);
        d_f[1] = uc_day1_cart_ratio = (double)(uc_day1_cart_ratio + 0.0001)/(uc_day1_view_count+uc_day1_collect_count+uc_day1_cart_count+uc_day1_buy_count + 0.0001);
        d_f[2] = uc_day1_buy_ratio = (double)(uc_day1_buy_ratio + 0.0001)/(uc_day1_view_count+uc_day1_collect_count+uc_day1_cart_count+uc_day1_buy_count + 0.0001);
        //d2
        i_f[12] = uc_day_2_behavior_cross_hours = (uc_day_bahavior_max_hour[1] == -1) ? 0 : uc_day_bahavior_max_hour[1] - uc_day_bahavior_min_hour[1] + 1;   
        i_f[13] = uc_day_2_view_cross_hours = (uc_day_view_max_hour[1] == -1) ? 0 : uc_day_view_max_hour[1] - uc_day_view_min_hour[1] + 1; 
        i_f[14] = uc_day_2_buy_cross_hours = (uc_day_buy_max_hour[1] == -1) ? 0 : uc_day_buy_max_hour[1] - uc_day_buy_min_hour[1] + 1;
        i_f[15] = uc_day_2_view_count = uc_day_view_count[1];
        i_f[16] = uc_day_2_collect_count = uc_day_collect_count[1];
        i_f[17] = uc_day_2_cart_count = uc_day_cart_count[1];
        i_f[18] = uc_day_2_buy_count = uc_day_buy_count[1];
        i_f[19] = uc_day_2_all_count = uc_day_behavior_count[1];
        i_f[20] = uc_day_2_cart_and_behavior_count = uc_day_2_cart_count * uc_day_2_all_count;//??????????????
        i_f[21] = uc_day_2_is_cart = (uc_day_2_cart_count > 0) ? 1:0;
        i_f[22] = uc_day_2_is_buy = (uc_day_2_buy_count > 0) ? 1:0;
        i_f[23] = uc_day_2_is_cart_not_buy = (uc_day_2_is_cart == 1 && uc_day_2_is_buy == 0) ? 1:0;
        d_f[3] = uc_day_2_collect_ratio = (double)(uc_day_2_collect_count + 0.0001)/(uc_day_2_view_count+uc_day_2_collect_count+uc_day_2_cart_count+uc_day_2_buy_count + 0.0001);
        d_f[4] = uc_day_2_cart_ratio = (double)(uc_day_2_collect_count + 0.0001)/(uc_day_2_view_count+uc_day_2_collect_count+uc_day_2_cart_count+uc_day_2_buy_count + 0.0001);
        d_f[5] = uc_day_2_buy_ratio = (double)(uc_day_2_collect_count + 0.0001)/(uc_day_2_view_count+uc_day_2_collect_count+uc_day_2_cart_count+uc_day_2_buy_count + 0.0001);
        //d3
        i_f[24] = uc_day_3_behavior_cross_hours = (uc_day_bahavior_max_hour[2] == -1) ? 0 : uc_day_bahavior_max_hour[2] - uc_day_bahavior_min_hour[2] + 1;   
        i_f[25] = uc_day_3_view_cross_hours = (uc_day_view_max_hour[2] == -1) ? 0 : uc_day_view_max_hour[2] - uc_day_view_min_hour[2] + 1; 
        i_f[26] = uc_day_3_buy_cross_hours = (uc_day_buy_max_hour[2] == -1) ? 0 : uc_day_buy_max_hour[2] - uc_day_buy_min_hour[2] + 1;
        i_f[27] = uc_day_3_view_count = uc_day_view_count[2];
        i_f[28] = uc_day_3_collect_count = uc_day_collect_count[2];
        i_f[29] = uc_day_3_cart_count = uc_day_cart_count[2];
        i_f[30] = uc_day_3_buy_count = uc_day_buy_count[2];
        i_f[31] = uc_day_3_all_count = uc_day_behavior_count[2];
        i_f[32] = uc_day_3_cart_and_behavior_count = uc_day_3_cart_count * uc_day_3_all_count;//??????????????
        i_f[33] = uc_day_3_is_cart = (uc_day_3_cart_count > 0) ? 1:0;
        i_f[34] = uc_day_3_is_buy = (uc_day_3_buy_count > 0) ? 1:0;
        i_f[35] = uc_day_3_is_cart_not_buy = (uc_day_3_is_cart == 1 && uc_day_3_is_buy == 0) ? 1:0;
        d_f[6] = uc_day_3_collect_ratio = (double)(uc_day_3_collect_count + 0.0001)/(uc_day_3_view_count+uc_day_3_collect_count+uc_day_3_cart_count+uc_day_3_buy_count + 0.0001);
        d_f[7] = uc_day_3_cart_ratio = (double)(uc_day_3_collect_count + 0.0001)/(uc_day_3_view_count+uc_day_3_collect_count+uc_day_3_cart_count+uc_day_3_buy_count + 0.0001);
        d_f[8] = uc_day_3_buy_ratio = (double)(uc_day_3_collect_count + 0.0001)/(uc_day_3_view_count+uc_day_3_collect_count+uc_day_3_cart_count+uc_day_3_buy_count + 0.0001);
        //d4
        i_f[36] = uc_day_4_behavior_cross_hours = (uc_day_bahavior_max_hour[3] == -1) ? 0 : uc_day_bahavior_max_hour[3] - uc_day_bahavior_min_hour[3] + 1;   
        i_f[37] = uc_day_4_view_cross_hours = (uc_day_view_max_hour[3] == -1) ? 0 : uc_day_view_max_hour[3] - uc_day_view_min_hour[3] + 1; 
        i_f[38] = uc_day_4_buy_cross_hours = (uc_day_buy_max_hour[3] == -1) ? 0 : uc_day_buy_max_hour[3] - uc_day_buy_min_hour[3] + 1;
        i_f[39] = uc_day_4_view_count = uc_day_view_count[3];
        i_f[40] = uc_day_4_collect_count = uc_day_collect_count[3];
        i_f[41] = uc_day_4_cart_count = uc_day_cart_count[3];
        i_f[42] = uc_day_4_buy_count = uc_day_buy_count[3];
        i_f[43] = uc_day_4_all_count = uc_day_behavior_count[3];
        i_f[44] = uc_day_4_cart_and_behavior_count = uc_day_4_cart_count * uc_day_4_all_count;//??????????????
        i_f[45] = uc_day_4_is_cart = (uc_day_4_cart_count > 0) ? 1:0;
        i_f[46] = uc_day_4_is_buy = (uc_day_4_buy_count > 0) ? 1:0;
        i_f[47] = uc_day_4_is_cart_not_buy = (uc_day_4_is_cart == 1 && uc_day_4_is_buy == 0) ? 1:0;
        d_f[9] = uc_day_4_collect_ratio = (double)(uc_day_4_collect_count + 0.0001)/(uc_day_4_view_count+uc_day_4_collect_count+uc_day_4_cart_count+uc_day_4_buy_count + 0.0001);
        d_f[10] = uc_day_4_cart_ratio = (double)(uc_day_4_collect_count + 0.0001)/(uc_day_4_view_count+uc_day_4_collect_count+uc_day_4_cart_count+uc_day_4_buy_count + 0.0001);
        d_f[11] = uc_day_4_buy_ratio = (double)(uc_day_4_collect_count + 0.0001)/(uc_day_4_view_count+uc_day_4_collect_count+uc_day_4_cart_count+uc_day_4_buy_count + 0.0001);
        //d5
        i_f[48] = uc_day_5_behavior_cross_hours = (uc_day_bahavior_max_hour[4] == -1) ? 0 : uc_day_bahavior_max_hour[4] - uc_day_bahavior_min_hour[4] + 1;   
        i_f[49] = uc_day_5_view_cross_hours = (uc_day_view_max_hour[4] == -1) ? 0 : uc_day_view_max_hour[4] - uc_day_view_min_hour[4] + 1; 
        i_f[50] = uc_day_5_buy_cross_hours = (uc_day_buy_max_hour[4] == -1) ? 0 : uc_day_buy_max_hour[4] - uc_day_buy_min_hour[4] + 1;
        i_f[51] = uc_day_5_view_count = uc_day_view_count[4];
        i_f[52] = uc_day_5_collect_count = uc_day_collect_count[4];
        i_f[53] = uc_day_5_cart_count = uc_day_cart_count[4];
        i_f[54] = uc_day_5_buy_count = uc_day_buy_count[4];
        i_f[55] = uc_day_5_all_count = uc_day_behavior_count[4];
        i_f[56] = uc_day_5_cart_and_behavior_count = uc_day_5_cart_count * uc_day_5_all_count;//??????????????
        i_f[57] = uc_day_5_is_cart = (uc_day_5_cart_count > 0) ? 1:0;
        i_f[58] = uc_day_5_is_buy = (uc_day_5_buy_count > 0) ? 1:0;
        i_f[59] = uc_day_5_is_cart_not_buy = (uc_day_5_is_cart == 1 && uc_day_5_is_buy == 0) ? 1:0;
        d_f[12] = uc_day_5_collect_ratio = (double)(uc_day_5_collect_count + 0.0001)/(uc_day_5_view_count+uc_day_5_collect_count+uc_day_5_cart_count+uc_day_5_buy_count + 0.0001);
        d_f[13] = uc_day_5_cart_ratio = (double)(uc_day_5_collect_count + 0.0001)/(uc_day_5_view_count+uc_day_5_collect_count+uc_day_5_cart_count+uc_day_5_buy_count + 0.0001);
        d_f[14] = uc_day_5_buy_ratio = (double)(uc_day_5_collect_count + 0.0001)/(uc_day_5_view_count+uc_day_5_collect_count+uc_day_5_cart_count+uc_day_5_buy_count + 0.0001);
        //d6
        i_f[60] = uc_day_6_behavior_cross_hours = (uc_day_bahavior_max_hour[5] == -1) ? 0 : uc_day_bahavior_max_hour[5] - uc_day_bahavior_min_hour[5] + 1;   
        i_f[61] = uc_day_6_view_cross_hours = (uc_day_view_max_hour[5] == -1) ? 0 : uc_day_view_max_hour[5] - uc_day_view_min_hour[5] + 1; 
        i_f[62] = uc_day_6_buy_cross_hours = (uc_day_buy_max_hour[5] == -1) ? 0 : uc_day_buy_max_hour[5] - uc_day_buy_min_hour[5] + 1;
        i_f[63] = uc_day_6_view_count = uc_day_view_count[5];
        i_f[64] = uc_day_6_collect_count = uc_day_collect_count[5];
        i_f[65] = uc_day_6_cart_count = uc_day_cart_count[5];
        i_f[66] = uc_day_6_buy_count = uc_day_buy_count[5];
        i_f[67] = uc_day_6_all_count = uc_day_behavior_count[5];
        i_f[68] = uc_day_6_cart_and_behavior_count = uc_day_6_cart_count * uc_day_6_all_count;//??????????????
        i_f[69] = uc_day_6_is_cart = (uc_day_6_cart_count > 0) ? 1:0;
        i_f[70] = uc_day_6_is_buy = (uc_day_6_buy_count > 0) ? 1:0;
        i_f[71] = uc_day_6_is_cart_not_buy = (uc_day_6_is_cart == 1 && uc_day_6_is_buy == 0) ? 1:0;
        d_f[15] = uc_day_6_collect_ratio = (double)(uc_day_6_collect_count + 0.0001)/(uc_day_6_view_count+uc_day_6_collect_count+uc_day_6_cart_count+uc_day_6_buy_count + 0.0001);
        d_f[16] = uc_day_6_cart_ratio = (double)(uc_day_6_collect_count + 0.0001)/(uc_day_6_view_count+uc_day_6_collect_count+uc_day_6_cart_count+uc_day_6_buy_count + 0.0001);
        d_f[17] = uc_day_6_buy_ratio = (double)(uc_day_6_collect_count + 0.0001)/(uc_day_6_view_count+uc_day_6_collect_count+uc_day_6_cart_count+uc_day_6_buy_count + 0.0001);
        //d7
        i_f[72] = uc_day_7_behavior_cross_hours = (uc_day_bahavior_max_hour[6] == -1) ? 0 : uc_day_bahavior_max_hour[6] - uc_day_bahavior_min_hour[6] + 1;   
        i_f[73] = uc_day_7_view_cross_hours = (uc_day_view_max_hour[6] == -1) ? 0 : uc_day_view_max_hour[6] - uc_day_view_min_hour[6] + 1; 
        i_f[74] = uc_day_7_buy_cross_hours = (uc_day_buy_max_hour[6] == -1) ? 0 : uc_day_buy_max_hour[6] - uc_day_buy_min_hour[6] + 1;
        i_f[75] = uc_day_7_view_count = uc_day_view_count[6];
        i_f[76] = uc_day_7_collect_count = uc_day_collect_count[6];
        i_f[77] = uc_day_7_cart_count = uc_day_cart_count[6];
        i_f[78] = uc_day_7_buy_count = uc_day_buy_count[6];
        i_f[79] = uc_day_7_all_count = uc_day_behavior_count[6];
        i_f[80] = uc_day_7_cart_and_behavior_count = uc_day_7_cart_count * uc_day_7_all_count;//??????????????
        i_f[81] = uc_day_7_is_cart = (uc_day_7_cart_count > 0) ? 1:0;
        i_f[82] = uc_day_7_is_buy = (uc_day_7_buy_count > 0) ? 1:0;
        i_f[83] = uc_day_7_is_cart_not_buy = (uc_day_7_is_cart == 1 && uc_day_7_is_buy == 0) ? 1:0;
        d_f[18] = uc_day_7_collect_ratio = (double)(uc_day_7_collect_count + 0.0001)/(uc_day_7_view_count+uc_day_7_collect_count+uc_day_7_cart_count+uc_day_7_buy_count + 0.0001);
        d_f[19] = uc_day_7_cart_ratio = (double)(uc_day_7_collect_count + 0.0001)/(uc_day_7_view_count+uc_day_7_collect_count+uc_day_7_cart_count+uc_day_7_buy_count + 0.0001);
        d_f[20] = uc_day_7_buy_ratio = (double)(uc_day_7_collect_count + 0.0001)/(uc_day_7_view_count+uc_day_7_collect_count+uc_day_7_cart_count+uc_day_7_buy_count + 0.0001);
        //d1-d3      
        i_f[84] = uc_day_1_3_behavior_cross_hours = (uc_day_bahavior_max_hour[7] == -1) ? 0 : uc_day_bahavior_max_hour[7] - uc_day_bahavior_min_hour[7] + 1;
        i_f[85] = uc_day_1_3_view_cross_days = (uc_day_view_max_dt[7] == -1) ? 0 : uc_day_view_max_dt[7] - uc_day_view_min_dt[7] + 1;
        i_f[86] = uc_day_1_3_buy_cross_days = (uc_day_buy_max_dt[7] == -1) ? 0 : uc_day_buy_max_dt[7] - uc_day_buy_min_dt[7] + 1;
        i_f[87] = uc_day_1_3_view_count = uc_day_view_count[7];
        i_f[88] = uc_day_1_3_collect_count = uc_day_collect_count[7];
        i_f[89] = uc_day_1_3_cart_count = uc_day_cart_count[7];
        i_f[90] = uc_day_1_3_buy_count = uc_day_buy_count[7];
        i_f[91] = uc_day_1_3_all_count = uc_day_view_count[7] + uc_day_collect_count[7] + uc_day_cart_count[7] + uc_day_buy_count[7];
        i_f[92] = uc_day_1_3_is_cart = (uc_day_1_3_cart_count > 0) ? 1:0;
        i_f[93] = uc_day_1_3_is_buy = (uc_day_1_3_buy_count > 0) ? 1:0;
        i_f[94] = uc_day_1_3_is_cart_not_buy = (uc_day_1_3_is_cart == 1 && uc_day_1_3_is_buy == 0) ? 1:0;
        i_f[95] = uc_day_1_3_cart_and_behavior_count = uc_day_1_3_cart_count * uc_day_1_3_all_count;
        d_f[21] = uc_day_1_3_buy_ratio = (double)(uc_day_1_3_buy_count + 0.0001)/(uc_day_1_3_all_count + 0.0001);
        d_f[22] = uc_day_1_3_collect_ratio = (double)(uc_day_1_3_collect_count + 0.0001)/(uc_day_1_3_all_count + 0.0001);
        d_f[23] = uc_day_1_3_cart_ratio = (double)(uc_day_1_3_cart_count + 0.0001)/(uc_day_1_3_all_count + 0.0001);
        //d1-d7
        i_f[96] = uc_day_1_7_behavior_cross_hours = (uc_day_bahavior_max_hour[8] == -1) ? 0 : uc_day_bahavior_max_hour[8] - uc_day_bahavior_min_hour[8] + 1;
        i_f[97] = uc_day_1_7_view_cross_days = (uc_day_view_max_dt[8] == -1) ? 0 : uc_day_view_max_dt[8] - uc_day_view_min_dt[8] + 1;
        i_f[98] = uc_day_1_7_buy_cross_days = (uc_day_buy_max_dt[8] == -1) ? 0 : uc_day_buy_max_dt[8] - uc_day_buy_min_dt[8] + 1;
        i_f[99] = uc_day_1_7_view_count = uc_day_view_count[8];
        i_f[100] = uc_day_1_7_collect_count = uc_day_collect_count[8];
        i_f[101] = uc_day_1_7_cart_count = uc_day_cart_count[8];
        i_f[102] = uc_day_1_7_buy_count = uc_day_buy_count[8];
        i_f[103] = uc_day_1_7_all_count = uc_day_view_count[8] + uc_day_collect_count[8] + uc_day_cart_count[8] + uc_day_buy_count[8];
        i_f[104] = uc_day_1_7_is_cart = (uc_day_1_7_cart_count > 0) ? 1:0;
        i_f[105] = uc_day_1_7_is_buy = (uc_day_1_7_buy_count > 0) ? 1:0;
        i_f[106] = uc_day_1_7_is_cart_not_buy = (uc_day_1_7_is_cart == 1 && uc_day_1_7_is_buy == 0) ? 1:0;
        i_f[107] = uc_day_1_7_cart_and_behavior_count = uc_day_1_7_cart_count * uc_day_1_7_all_count;
        d_f[24] = uc_day_1_7_buy_ratio = (double)(uc_day_1_7_buy_count + 0.0001)/(uc_day_1_7_all_count + 0.0001);
        d_f[25] = uc_day_1_7_collect_ratio = (double)(uc_day_1_7_collect_count + 0.0001)/(uc_day_1_7_all_count + 0.0001);
        d_f[26] = uc_day_1_7_cart_ratio = (double)(uc_day_1_7_cart_count + 0.0001)/(uc_day_1_7_all_count + 0.0001);
        //d1-d14
        i_f[108] = uc_day_1_14_behavior_cross_hours = (uc_day_bahavior_max_hour[9] == -1) ? 0 : uc_day_bahavior_max_hour[9] - uc_day_bahavior_min_hour[9] + 1;
        i_f[109] = uc_day_1_14_view_cross_days = (uc_day_view_max_dt[9] == -1) ? 0 : uc_day_view_max_dt[9] - uc_day_view_min_dt[9] + 1;
        i_f[110] = uc_day_1_14_buy_cross_days = (uc_day_buy_max_dt[9] == -1) ? 0 : uc_day_buy_max_dt[9] - uc_day_buy_min_dt[9] + 1;
        i_f[111] = uc_day_1_14_view_count = uc_day_view_count[9];
        i_f[112] = uc_day_1_14_collect_count = uc_day_collect_count[9];
        i_f[113] = uc_day_1_14_cart_count = uc_day_cart_count[9];
        i_f[114] = uc_day_1_14_buy_count = uc_day_buy_count[9];
        i_f[115] = uc_day_1_14_all_count = uc_day_view_count[9] + uc_day_collect_count[9] + uc_day_cart_count[9] + uc_day_buy_count[9];
        i_f[116] = uc_day_1_14_is_cart = (uc_day_1_14_cart_count > 0) ? 1:0;
        i_f[117] = uc_day_1_14_is_buy = (uc_day_1_14_buy_count > 0) ? 1:0;
        i_f[118] = uc_day_1_14_is_cart_not_buy = (uc_day_1_14_is_cart == 1 && uc_day_1_14_is_buy == 0) ? 1:0;
        i_f[119] = uc_day_1_14_cart_and_behavior_count = uc_day_1_14_cart_count * uc_day_1_14_all_count;
        d_f[27] = uc_day_1_14_buy_ratio = (double)(uc_day_1_14_buy_count + 0.0001)/(uc_day_1_14_all_count + 0.0001);
        d_f[28] = uc_day_1_14_collect_ratio = (double)(uc_day_1_14_collect_count + 0.0001)/(uc_day_1_14_all_count + 0.0001);
        d_f[29] = uc_day_1_14_cart_ratio = (double)(uc_day_1_14_cart_count + 0.0001)/(uc_day_1_14_all_count + 0.0001);
        //d2-d3
        i_f[120] = uc_day_2_3_view_count = uc_day_2_view_count + uc_day_3_view_count;
        i_f[121] = uc_day_2_3_collect_count = uc_day_2_collect_count + uc_day_3_collect_count;
        i_f[122] = uc_day_2_3_cart_count = uc_day_2_cart_count + uc_day_3_cart_count;
        i_f[123] = uc_day_2_3_buy_count = uc_day_2_buy_count + uc_day_3_buy_count;
        i_f[124] = uc_day_2_3_all_count = uc_day_2_3_view_count + uc_day_2_3_collect_count + uc_day_2_3_cart_count + uc_day_2_3_buy_count;
        i_f[125] = uc_day_2_3_is_cart = (uc_day_2_3_cart_count > 0)? 1:0;
        i_f[126] = uc_day_2_3_is_buy = (uc_day_2_3_buy_count > 0)? 1:0;
        i_f[127] = uc_day_2_3_is_cart_not_buy = (uc_day_2_3_is_cart == 1 && uc_day_2_3_is_buy == 0) ? 1:0;
        i_f[128] = uc_day_2_3_behavior_cross_hours = (uc_day_bahavior_max_hour[12] == -1) ? 0 : uc_day_bahavior_max_hour[12] - uc_day_bahavior_min_hour[12] + 1;
        i_f[129] = uc_day_2_3_cart_and_behavior_count = uc_day_2_3_cart_count * uc_day_2_3_all_count;
        d_f[30] = uc_day_2_3_buy_ratio = (double)(uc_day_2_3_buy_count + 0.0001)/(uc_day_2_3_all_count + 0.0001);
        d_f[31] = uc_day_2_3_collect_ratio = (double)(uc_day_2_3_collect_count + 0.0001)/(uc_day_2_3_all_count + 0.0001);
        d_f[32] = uc_day_2_3_cart_ratio = (double)(uc_day_2_3_cart_count + 0.0001)/(uc_day_2_3_all_count + 0.0001);
        //d4-d7
        i_f[130] = uc_day_4_7_view_count = uc_day_1_7_view_count - uc_day_1_3_view_count;
        i_f[131] = uc_day_4_7_collect_count = uc_day_1_7_collect_count - uc_day_1_3_collect_count;
        i_f[132] = uc_day_4_7_cart_count = uc_day_1_7_cart_count - uc_day_1_3_cart_count;
        i_f[133] = uc_day_4_7_buy_count = uc_day_1_7_buy_count - uc_day_1_3_buy_count;
        i_f[134] = uc_day_4_7_all_count = uc_day_1_7_all_count - uc_day_1_3_all_count;
        i_f[135] = uc_day_4_7_is_cart = (uc_day_4_7_cart_count > 0)? 1:0;
        i_f[136] = uc_day_4_7_is_buy = (uc_day_4_7_buy_count > 0)? 1:0;
        i_f[137] = uc_day_4_7_is_cart_not_buy = (uc_day_4_7_is_cart == 1 && uc_day_4_7_is_buy == 0) ? 1:0;
        i_f[138] = uc_day_4_7_behavior_cross_hours = (uc_day_bahavior_max_hour[13] == -1) ? 0 : uc_day_bahavior_max_hour[13] - uc_day_bahavior_min_hour[13] + 1;
        i_f[139] = uc_day_4_7_cart_and_behavior_count = uc_day_4_7_cart_count * uc_day_4_7_all_count;
        d_f[33] = uc_day_4_7_buy_ratio = (double)(uc_day_4_7_buy_count + 0.0001)/(uc_day_4_7_all_count + 0.0001);
        d_f[34] = uc_day_4_7_collect_ratio = (double)(uc_day_4_7_collect_count + 0.0001)/(uc_day_4_7_all_count + 0.0001);
        d_f[35] = uc_day_4_7_cart_ratio = (double)(uc_day_4_7_cart_count + 0.0001)/(uc_day_4_7_all_count + 0.0001);
        //d8-d14
        i_f[140] = uc_day_8_14_view_count = uc_day_view_count[9] - uc_day_view_count[8];
        i_f[141] = uc_day_8_14_collect_count = uc_day_collect_count[9] - uc_day_collect_count[8];
        i_f[142] = uc_day_8_14_cart_count = uc_day_cart_count[9] - uc_day_cart_count[8];
        i_f[143] = uc_day_8_14_buy_count = uc_day_buy_count[9] - uc_day_buy_count[8];
        i_f[144] = uc_day_8_14_all_count = uc_day_behavior_count[9] - uc_day_behavior_count[8];
        i_f[145] = uc_day_8_14_behavior_cross_hours = (uc_day_bahavior_max_hour[10] == -1) ? 0 : uc_day_bahavior_max_hour[10] - uc_day_bahavior_min_hour[10] + 1;
        i_f[146] = uc_day_8_14_cart_and_behavior_count = uc_day_8_14_cart_count * uc_day_8_14_all_count;
        i_f[147] = uc_day_8_14_is_cart = (uc_day_8_14_cart_count > 0) ? 1:0;
        i_f[148] = uc_day_8_14_is_buy = (uc_day_8_14_buy_count > 0) ? 1:0;
        i_f[149] = uc_day_8_14_is_cart_not_buy = (uc_day_8_14_cart_count == 1 && uc_day_8_14_buy_count == 0) ? 1:0;
        d_f[36] = uc_day_8_14_buy_ratio = (double)(uc_day_8_14_buy_count + 0.0001)/(uc_day_8_14_all_count + 0.0001);
        d_f[37] = uc_day_8_14_collect_ratio = (double)(uc_day_8_14_collect_count + 0.0001)/(uc_day_8_14_all_count + 0.0001);
        d_f[38] = uc_day_8_14_cart_ratio = (double)(uc_day_8_14_cart_count + 0.0001)/(uc_day_8_14_all_count + 0.0001);
        //some ratio
        d_f[39] = uc_day1to2_3_cart_ratio = (double)(uc_day1_cart_count + 0.0001)/(uc_day_2_3_cart_count + 0.0001);	
        d_f[40] = uc_day_1_3to4_7_cart_ratio = (double)(uc_day_1_3_cart_count + 0.0001)/(uc_day_4_7_cart_count + 0.0001);
        d_f[41] = uc_day_1_7to8_14cart_ratio = (double)(uc_day_1_7_cart_count + 0.0001)/(uc_day_1_14_cart_count + 0.0001);	
    	//some connection       
        i_f[150] = tuc_day2_continuous_buy = (uc_day_buy_count[0] > 0 && uc_day_buy_count[1] > 0) ? 1:0;
        i_f[151] = tuc_day3_continuous_buy = (uc_day_buy_count[0] + uc_day_buy_count[1] + uc_day_buy_count[2] > 1) ? 1:0;
        i_f[152] = tuc_pre_3_view_1_cart = (uc_day_view_count[7]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[153] = tuc_pre_7_view_1_cart = (uc_day_view_count[8]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[154] = tuc_pre_3_collect_1_cart = (uc_day_collect_count[7]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[155] = tuc_pre_7_collect_1_cart = (uc_day_collect_count[8]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[156] = tuc_pre_3_cart_1_cart = (uc_day_cart_count[7]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[157] = tuc_pre_7_cart_1_cart = (uc_day_cart_count[8]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[158] = tuc_pre_3_buy_1_cart = (uc_day_buy_count[7]>0 && uc_day_cart_count[0]>0) ? 1:0;
        i_f[159] = tuc_pre_7_buy_1_cart = (uc_day_cart_count[8]>0 && uc_day_cart_count[0]>0) ? 1:0;  
        i_f[160] = uc_behavior_days = set_uc_behavior_days.size();
        i_f[161] = uc_behavior_hours = set_uc_behavior_hours.size();
        
        result.set(0,key.getString("uid"));
        result.set(1,key.getString("ic"));
        int counter =2 ;
        
        for (int i = 0; i < 162; i++)
        {
        	result.set(i+2, i_f[i]); counter++;
        }
        for (int j = 0; j < 42; j++)
        {
        	result.set(j + 2 + 162, d_f[j]);counter++;
        }
        
        // by lemon 
        ArrayList lm_fList = new ArrayList();
        lm_getFeatures();
        
        // lm_fList.add(uc_lately_tp_dnt_hr[2]);
        lm_fList.add(uc_lately_tp_dnt_hr[3]);
        
        for(Iterator it = lm_fList.iterator();it.hasNext();){
            result.set( counter ,it.next());
            counter ++;
       }
        
        
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {
    }    
    
    
    // by lemon 
    private int[]  uc_lately_tp_dnt_hr ;
    private boolean[] ever_tp_bool ;
    
    public void lm_makingFeatures(String iid , String ug ,int tp , int timeDistance,int hr ){
    	int d = timeDistance * 24 - hr ;
    	if( uc_lately_tp_dnt_hr[tp-1] > d  ){
    		ever_tp_bool[tp-1] = true ;
    		uc_lately_tp_dnt_hr[tp-1] = d  ;
    	}
    }
    
    public void lm_getFeatures(){
    	
    	for(int i = 0 ;i <4;i++){
    		if(ever_tp_bool[i] == false )uc_lately_tp_dnt_hr[i] = 0 ;
    	}
    }
    
    public void lm_init(){
    	uc_lately_tp_dnt_hr = new int[4];
    	for(int i = 0 ; i <4; i++){
    		uc_lately_tp_dnt_hr[i] = 24*31 ;
    	}
    	ever_tp_bool = new boolean[4] ;
    }
    
    
}
