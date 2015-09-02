package wonder.mapreduce.ui;

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
//    offsize=0时，1118-1217
//    offsize=1时，1119-1218
    private static int offsize = 1 ;
    
    
    //d1
    //////////////////////////////
    private static int ui_day1_view_cnt[] = {0, 0, 0, 0};//1h, 3h, 6h, 12h
    private static int ui_day1_collect_cnt[] = {0, 0, 0, 0};//1h, 3h, 6h, 12h
    private static int ui_day1_cart_cnt[] = {0, 0, 0, 0};//1h, 3h, 6h, 12h
    private static int ui_day1_buy_cnt[] = {0, 0, 0, 0};//1h, 3h, 6h, 12h
    private static int ui_day1_behavior_cnt[] = {0, 0, 0, 0};//1h, 3h, 6h, 12h
    //////////////////////////////
    private int ui_day1_behavior_cross_hours;
    private int ui_day1_view_cross_hours;
    private int ui_day1_buy_cross_hours;
    private int ui_day1_view_count;
    private int ui_day1_collect_count;
    private int ui_day1_cart_count;
    private int ui_day1_buy_count;
    private int ui_day1_behavior_count;
    private int ui_day1_cart_and_behavior_count;
    private int ui_day1_is_cart;
    private int ui_day1_is_buy;
    private int ui_day1_is_cart_not_buy;
    //d2
    private int ui_day_2_behavior_cross_hours;
    private int ui_day_2_view_cross_hours;
    private int ui_day_2_buy_cross_hours;
    private int ui_day_2_view_count;
    private int ui_day_2_collect_count;
    private int ui_day_2_cart_count;
    private int ui_day_2_buy_count;
    private int ui_day_2_behavior_count;
    private int ui_day_2_cart_and_behavior_count;
    private int ui_day_2_is_cart;
    private int ui_day_2_is_buy;
    private int ui_day_2_is_cart_not_buy;
    //d3
    private int ui_day_3_behavior_cross_hours;
    private int ui_day_3_view_cross_hours;
    private int ui_day_3_buy_cross_hours;
    private int ui_day_3_view_count;
    private int ui_day_3_collect_count;
    private int ui_day_3_cart_count;
    private int ui_day_3_buy_count;
    private int ui_day_3_behavior_count;
    private int ui_day_3_cart_and_behavior_count;
    private int ui_day_3_is_cart;
    private int ui_day_3_is_buy;
    private int ui_day_3_is_cart_not_buy;
    //d4
    private int ui_day_4_behavior_cross_hours;
    private int ui_day_4_view_cross_hours;
    private int ui_day_4_buy_cross_hours;
    private int ui_day_4_view_count;
    private int ui_day_4_collect_count;
    private int ui_day_4_cart_count;
    private int ui_day_4_buy_count;
    private int ui_day_4_behavior_count;
    private int ui_day_4_cart_and_behavior_count;
    private int ui_day_4_is_cart;
    private int ui_day_4_is_buy;
    private int ui_day_4_is_cart_not_buy;
    //d5
    private int ui_day_5_behavior_cross_hours;
    private int ui_day_5_view_cross_hours;
    private int ui_day_5_buy_cross_hours;
    private int ui_day_5_view_count;
    private int ui_day_5_collect_count;
    private int ui_day_5_cart_count;
    private int ui_day_5_buy_count;
    private int ui_day_5_behavior_count;
    private int ui_day_5_cart_and_behavior_count;
    private int ui_day_5_is_cart;
    private int ui_day_5_is_buy;
    private int ui_day_5_is_cart_not_buy;
    //d6
    private int ui_day_6_behavior_cross_hours;
    private int ui_day_6_view_cross_hours;
    private int ui_day_6_buy_cross_hours;
    private int ui_day_6_view_count;
    private int ui_day_6_collect_count;
    private int ui_day_6_cart_count;
    private int ui_day_6_buy_count;
    private int ui_day_6_behavior_count;
    private int ui_day_6_cart_and_behavior_count;
    private int ui_day_6_is_cart;
    private int ui_day_6_is_buy;
    private int ui_day_6_is_cart_not_buy;
    //d7
    private int ui_day_7_behavior_cross_hours;
    private int ui_day_7_view_cross_hours;
    private int ui_day_7_buy_cross_hours;
    private int ui_day_7_view_count;
    private int ui_day_7_collect_count;
    private int ui_day_7_cart_count;
    private int ui_day_7_buy_count;
    private int ui_day_7_behavior_count;
    private int ui_day_7_cart_and_behavior_count;
    private int ui_day_7_is_cart;
    private int ui_day_7_is_buy;
    private int ui_day_7_is_cart_not_buy;
    
    private int tui_day2_continuous_buy;
    private int tui_day3_continuous_buy;
    private int tui_pre_3_view_1_cart;
    private int tui_pre_7_view_1_cart;
    private int tui_pre_3_collect_1_cart;
    private int tui_pre_7_collect_1_cart;
    private int tui_pre_3_cart_1_cart;
    private int tui_pre_7_cart_1_cart;
    private int tui_pre_3_buy_1_cart;
    private int tui_pre_7_buy_1_cart;
    //d1-d3
    private int ui_day_1_3_bahavior_cross_hours = 0;
    private int ui_day_1_3_view_cross_hours = 0;
    private int ui_day_1_3_buy_cross_hours = 0;
    private int ui_day_1_3_bahavior_cross_days = 0;
    private int ui_day_1_3_view_cross_days;
    private int ui_day_1_3_buy_cross_days;
    private int ui_day_1_3_view_count;
    private int ui_day_1_3_collect_count;
    private int ui_day_1_3_cart_count;
    private int ui_day_1_3_buy_count;
    private int ui_day_1_3_behavior_count;
    //d1-d7
    private int ui_day_1_7_bahavior_cross_hours = 0;
    private int ui_day_1_7_view_cross_hours = 0;
    private int ui_day_1_7_buy_cross_hours = 0;
    private int ui_day_1_7_bahavior_cross_days = 0;
    private int ui_day_1_7_view_cross_days;
    private int ui_day_1_7_buy_cross_days;
    private int ui_day_1_7_view_count;
    private int ui_day_1_7_collect_count;
    private int ui_day_1_7_cart_count;
    private int ui_day_1_7_buy_count;
    private int ui_day_1_7_behavior_count;
    //d1-d14
    private int ui_day_1_14_bahavior_cross_hours = 0;
    private int ui_day_1_14_view_cross_hours = 0;
    private int ui_day_1_14_buy_cross_hours = 0;
    private int ui_day_1_14_bahavior_cross_days = 0;
    private int ui_day_1_14_view_cross_days;
    private int ui_day_1_14_buy_cross_days;
    private int ui_day_1_14_view_count;
    private int ui_day_1_14_collect_count;
    private int ui_day_1_14_cart_count;
    private int ui_day_1_14_buy_count;
    private int ui_day_1_14_behavior_count;
    //d2-d3
    private int ui_day_2_3_view_count;
    private int ui_day_2_3_collect_count;
    private int ui_day_2_3_cart_count;
    private int ui_day_2_3_buy_count;
    private int ui_day_2_3_behavior_count;
    //d4-d7
    private int ui_day_4_7_view_count;
    private int ui_day_4_7_collect_count;
    private int ui_day_4_7_cart_count;
    private int ui_day_4_7_buy_count;
    private int ui_day_4_7_behavior_count;
    //d8-d14
    private int ui_day_8_14_view_count;
    private int ui_day_8_14_collect_count;
    private int ui_day_8_14_cart_count;
    private int ui_day_8_14_buy_count;
    private int ui_day_8_14_behavior_count;
    private int ui_day_8_14_behavior_cross_hours;
    private int ui_day_8_14_behavior_cross_days;
    private int ui_day_8_14_is_cart;
    private int ui_day_8_14_is_buy;
    private int ui_day_8_14_is_cart_not_buy;
    //sth important
    private int ui_last_behavior_dis_hours;
    private int ui_last_behavior_dis_days;
    private int ui_last_behavior_type;
    private int ui_last_view_dis_hours;
    private int ui_last_buy_dis_hours;
    private int ui_is_ever_buy;
    private int ui_buy_dis_last_behavior_hours;
    private int ui_buy_dis_last_behavior_days;
    private int ui_all_view_count = 0;
    private int ui_all_collect_count = 0;
    private int ui_all_cart_count = 0;
    private int ui_all_buy_count = 0;
    private int ui_all_behavior_count = 0;
    private int ui_is_ever_view = 0;
    private int ui_is_ever_collect = 0;
    private int ui_is_ever_cart = 0;
    private int ui_behavior_days;
    private int ui_behavior_hours;
    
    private double ui_c_day1_view_count_ratio;
    private double ui_c_day1_behavior_count_ratio;
    private double ui_c_day3_view_count_ratio;
    private double ui_c_day3_behavior_count_ratio;
    private double ui_c_day7_view_count_ratio;
    private double ui_c_day7_behavior_count_ratio;
    private double ui_c_day14_view_count_ratio;
    private double ui_c_day14_behavior_count_ratio;
    
    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }
    
    public void init()
    {
    	//d1
    	for (int i = 0; i < 4; i++)
    	{
    		ui_day1_view_cnt[i] = 0;
    		ui_day1_collect_cnt[i] = 0;
    		ui_day1_cart_cnt[i] = 0;
    		ui_day1_buy_cnt[i] = 0;
    		ui_day1_behavior_cnt[i] = 0;
    	}
    	ui_day1_behavior_cross_hours = 0;
        ui_day1_view_cross_hours = 0;
        ui_day1_buy_cross_hours = 0;
        ui_day1_view_count = 0;
        ui_day1_collect_count = 0;
        ui_day1_cart_count = 0;
        ui_day1_buy_count = 0;
        ui_day1_behavior_count = 0;
        ui_day1_cart_and_behavior_count = 0;
        ui_day1_is_cart = 0;
        ui_day1_is_buy = 0;
        ui_day1_is_cart_not_buy = 0;
        //d2
        ui_day_2_behavior_cross_hours = 0;
        ui_day_2_view_cross_hours = 0;
        ui_day_2_buy_cross_hours = 0;
        ui_day_2_view_count = 0;
        ui_day_2_collect_count = 0;
        ui_day_2_cart_count = 0;
        ui_day_2_buy_count = 0;
        ui_day_2_behavior_count = 0;
        ui_day_2_behavior_cross_hours = 0;
        ui_day_2_is_cart = 0;
        ui_day_2_is_buy = 0;
        ui_day_2_is_cart_not_buy = 0;
        //d3
        ui_day_3_behavior_cross_hours = 0;
        ui_day_3_view_cross_hours = 0;
        ui_day_3_buy_cross_hours = 0;
        ui_day_3_view_count = 0;
        ui_day_3_collect_count = 0;
        ui_day_3_cart_count = 0;
        ui_day_3_buy_count = 0;
        ui_day_3_behavior_count = 0;
        ui_day_3_behavior_cross_hours = 0;
        ui_day_3_is_cart = 0;
        ui_day_3_is_buy = 0;
        ui_day_3_is_cart_not_buy = 0;
        //d4
        ui_day_4_behavior_cross_hours = 0;
        ui_day_4_view_cross_hours = 0;
        ui_day_4_buy_cross_hours = 0;
        ui_day_4_view_count = 0;
        ui_day_4_collect_count = 0;
        ui_day_4_cart_count = 0;
        ui_day_4_buy_count = 0;
        ui_day_4_behavior_count = 0;
        ui_day_4_behavior_cross_hours = 0;
        ui_day_4_is_cart = 0;
        ui_day_4_is_buy = 0;
        ui_day_4_is_cart_not_buy = 0;
        //d5
        ui_day_5_behavior_cross_hours = 0;
        ui_day_5_view_cross_hours = 0;
        ui_day_5_buy_cross_hours = 0;
        ui_day_5_view_count = 0;
        ui_day_5_collect_count = 0;
        ui_day_5_cart_count = 0;
        ui_day_5_buy_count = 0;
        ui_day_5_behavior_count = 0;
        ui_day_5_behavior_cross_hours = 0;
        ui_day_5_is_cart = 0;
        ui_day_5_is_buy = 0;
        ui_day_5_is_cart_not_buy = 0;
        //d6
        ui_day_6_behavior_cross_hours = 0;
        ui_day_6_view_cross_hours = 0;
        ui_day_6_buy_cross_hours = 0;
        ui_day_6_view_count = 0;
        ui_day_6_collect_count = 0;
        ui_day_6_cart_count = 0;
        ui_day_6_buy_count = 0;
        ui_day_6_behavior_count = 0;
        ui_day_6_behavior_cross_hours = 0;
        ui_day_6_is_cart = 0;
        ui_day_6_is_buy = 0;
        ui_day_6_is_cart_not_buy = 0;
        //d7
        ui_day_7_behavior_cross_hours = 0;
        ui_day_7_view_cross_hours = 0;
        ui_day_7_buy_cross_hours = 0;
        ui_day_7_view_count = 0;
        ui_day_7_collect_count = 0;
        ui_day_7_cart_count = 0;
        ui_day_7_buy_count = 0;
        ui_day_7_behavior_count = 0;
        ui_day_7_behavior_cross_hours = 0;
        ui_day_7_is_cart = 0;
        ui_day_7_is_buy = 0;
        ui_day_7_is_cart_not_buy = 0;
         
        //d1-d3
        ui_day_1_3_bahavior_cross_hours = 0;
        ui_day_1_3_view_cross_hours = 0;
        ui_day_1_3_buy_cross_hours = 0;
        ui_day_1_3_bahavior_cross_days = 0;
        ui_day_1_3_view_cross_days = 0;
        ui_day_1_3_buy_cross_days = 0;
        ui_day_1_3_view_count = 0;
        ui_day_1_3_collect_count = 0;
        ui_day_1_3_cart_count = 0;
        ui_day_1_3_buy_count = 0;
        ui_day_1_3_behavior_count = 0;
        //d1-d7
        ui_day_1_7_bahavior_cross_hours = 0;
        ui_day_1_7_view_cross_hours = 0;
        ui_day_1_7_buy_cross_hours = 0;
        ui_day_1_7_bahavior_cross_days = 0;
        ui_day_1_7_view_cross_days = 0;
        ui_day_1_7_buy_cross_days = 0;
        ui_day_1_7_view_count = 0;
        ui_day_1_7_collect_count = 0;
        ui_day_1_7_cart_count = 0;
        ui_day_1_7_buy_count = 0;
        ui_day_1_7_behavior_count = 0;
        //d1-d14
        ui_day_1_14_bahavior_cross_hours = 0;
        ui_day_1_14_view_cross_hours = 0;
        ui_day_1_14_buy_cross_hours = 0;
        ui_day_1_14_bahavior_cross_days = 0;
        ui_day_1_14_view_cross_days = 0;
        ui_day_1_14_buy_cross_days = 0;
        ui_day_1_14_view_count = 0;
        ui_day_1_14_collect_count = 0;
        ui_day_1_14_cart_count = 0;
        ui_day_1_14_buy_count = 0;
        ui_day_1_14_behavior_count = 0;     
        //d2-d3
        ui_day_2_3_view_count = 0;
        ui_day_2_3_collect_count = 0;
        ui_day_2_3_cart_count = 0;
        ui_day_2_3_buy_count = 0;
        ui_day_2_3_behavior_count = 0;
        //d4-d7
        ui_day_4_7_view_count = 0;
        ui_day_4_7_collect_count = 0;
        ui_day_4_7_cart_count = 0;
        ui_day_4_7_buy_count = 0;
        ui_day_4_7_behavior_count = 0;
        //d8-d14
        ui_day_8_14_view_count = 0;
        ui_day_8_14_collect_count = 0;
        ui_day_8_14_cart_count = 0;
        ui_day_8_14_buy_count = 0;
        ui_day_8_14_behavior_count = 0;
        ui_day_8_14_behavior_cross_hours = 0;
        ui_day_8_14_behavior_cross_days = 0;
        ui_day_8_14_is_cart = 0;
        ui_day_8_14_is_buy = 0;
        ui_day_8_14_is_cart_not_buy = 0;
        //sth important
        ui_last_behavior_dis_hours = 30*24;
        ui_last_behavior_dis_days = 30;
        ui_last_behavior_type = 0;
        ui_last_view_dis_hours = 30*24;
        ui_last_buy_dis_hours = 30*24;
        ui_is_ever_buy = 0;
        ui_buy_dis_last_behavior_hours = 0;
        ui_buy_dis_last_behavior_days = 0;
        ui_all_view_count = 0;
        ui_all_collect_count = 0;
        ui_all_cart_count = 0;
        ui_all_buy_count = 0;
        ui_all_behavior_count = 0;
        ui_is_ever_view = 0;
        ui_is_ever_collect = 0;
        ui_is_ever_cart = 0;
        ui_behavior_days = 0;
        ui_behavior_hours = 0;
        
        ui_c_day1_view_count_ratio = 0;
        ui_c_day1_behavior_count_ratio = 0;
        ui_c_day3_view_count_ratio = 0;
        ui_c_day3_behavior_count_ratio = 0;
        ui_c_day7_view_count_ratio = 0;
        ui_c_day7_behavior_count_ratio = 0;
        ui_c_day14_view_count_ratio = 0;
        ui_c_day14_behavior_count_ratio = 0;
        
        tui_day2_continuous_buy = 0;
        tui_day3_continuous_buy = 0;
        tui_pre_3_view_1_cart = 0;
        tui_pre_7_view_1_cart = 0;
        tui_pre_3_collect_1_cart = 0;
        tui_pre_7_collect_1_cart = 0;
        tui_pre_3_cart_1_cart = 0;
        tui_pre_7_cart_1_cart = 0;
        tui_pre_3_buy_1_cart = 0;
        tui_pre_7_buy_1_cart = 0;
        
        for (int i = 0; i < 12; i++)
        {
        	ui_day_bahavior_max_hour[i] = -1;
            ui_day_bahavior_min_hour[i] = -1;
            ui_day_bahavior_max_dt[i] = -1;
            ui_day_bahavior_min_dt[i] = -1;
            ui_day_view_max_hour[i] = -1;
            ui_day_view_min_hour[i] = -1;
            ui_day_view_max_dt[i] = -1;
            ui_day_view_min_dt[i] = -1;
            ui_day_buy_max_hour[i] = -1;
            ui_day_buy_min_hour[i] = -1;
            ui_day_buy_max_dt[i] = -1;
            ui_day_buy_min_dt[i] = -1;    
            ui_day_view_count[i] = 0;
            ui_day_collect_count[i] = 0;
            ui_day_cart_count[i] = 0;
            ui_day_buy_count[i] = 0;
            ui_day_behavior_count[i] = 0;
        }
        set_ui_behavior_days.clear();
        set_ui_behavior_hours.clear();
    }
    //////////////////////////////////////////////////1   2   3   4   5   6   7 1-3  1-7 1-14 8-14 all
    private static int ui_day_bahavior_max_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1, -1,  -1,  -1, -1};//1/2/3/4/5/6/7/1-3/1-7/1-14/8-14/all;
    private static int ui_day_bahavior_min_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_bahavior_max_dt[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_bahavior_min_dt[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_view_max_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};//1/2/3/4/5/6/7/1-3/1-7/1-14/8-14/all;
    private static int ui_day_view_min_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_view_max_dt[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_view_min_dt[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_buy_max_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};//1/2/3/4/5/6/7/1-3/1-7/1-14/8-14/all;
    private static int ui_day_buy_min_hour[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_buy_max_dt[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};
    private static int ui_day_buy_min_dt[] = {-1, -1, -1, -1, -1, -1, -1, -1,-1,-1,-1,-1};    
    private static int ui_day_view_count[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};//1/2/3/4/5/6/7/1-3/1-7/1-14/8-14/all;
    private static int ui_day_collect_count[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static int ui_day_cart_count[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static int ui_day_buy_count[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private static int ui_day_behavior_count[] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    private Set set_ui_behavior_days = new HashSet();
    private Set set_ui_behavior_hours = new HashSet();
    

    
    

    public void genFeature(int tp, int hr, int dt_m, int dt_d)
    {
    	int dis_hr = (17 + offsize - dt_d) * 24 + (24 - hr);
    	int dis_dt_d = 18 + offsize - dt_d;	
    	//d1
    	if (dt_m == 12 && dt_d > 16 + offsize && dt_d < 18 + offsize)
    	{
    		//hour features:1h, 3h, 6h, 12h
    		switch(tp)
    		{
    		case 1:
    			if (hr > 22)
    				ui_day1_view_cnt[0] += 1;
    			if (hr > 20)
    				ui_day1_view_cnt[1] += 1;
    			if (hr > 17)
    				ui_day1_view_cnt[2] += 1;
    			if (hr > 11)
    				ui_day1_view_cnt[3] += 1;
    			break;
    		case 2:
    			if (hr > 22)
    				ui_day1_collect_cnt[0] += 1;
    			if (hr > 20)
    				ui_day1_collect_cnt[1] += 1;
    			if (hr > 17)
    				ui_day1_collect_cnt[2] += 1;
    			if (hr > 11)
    				ui_day1_collect_cnt[3] += 1;
    			break;
    		case 3:
    			if (hr > 22)
    				ui_day1_cart_cnt[0] += 1;
    			if (hr > 20)
    				ui_day1_cart_cnt[1] += 1;
    			if (hr > 17)
    				ui_day1_cart_cnt[2] += 1;
    			if (hr > 11)
    				ui_day1_cart_cnt[3] += 1;
    			break;
    		case 4:
    			if (hr > 22)
    				ui_day1_buy_cnt[0] += 1;
    			if (hr > 20)
    				ui_day1_buy_cnt[1] += 1;
    			if (hr > 17)
    				ui_day1_buy_cnt[2] += 1;
    			if (hr > 11)
    				ui_day1_buy_cnt[3] += 1;
    			break;
    		}
    		if (hr > 22)
				ui_day1_behavior_cnt[0] += 1;
			if (hr > 20)
				ui_day1_behavior_cnt[1] += 1;
			if (hr > 17)
				ui_day1_behavior_cnt[2] += 1;
			if (hr > 11)
				ui_day1_behavior_cnt[3] += 1;
    		
    		//hour
    		if (ui_day_bahavior_max_hour[0] == -1)
    		{
    			ui_day_bahavior_max_hour[0] = ui_day_bahavior_min_hour[0] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[0] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[0] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[0] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[0] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[0] == -1)
        		{
        			ui_day_view_max_hour[0] = ui_day_view_min_hour[0] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[0] < dis_hr)
        			{
        				ui_day_view_max_hour[0] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[0] > dis_hr)
        			{
        				ui_day_view_min_hour[0] = dis_hr;
        			}
        		}
    			ui_day_view_count[0] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[0] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[0] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[0] == -1)
        		{
        			ui_day_buy_max_hour[0] = ui_day_buy_min_hour[0] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[0] < dis_hr)
        			{
        				ui_day_buy_max_hour[0] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[0] > dis_hr)
        			{
        				ui_day_buy_min_hour[0] = dis_hr;
        			}
        		}
    			ui_day_buy_count[0] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[0] += 1;
    	}
    	//d2
    	if (dt_m == 12 && dt_d > 15 + offsize && dt_d < 17 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[1] == -1)
    		{
    			ui_day_bahavior_max_hour[1] = ui_day_bahavior_min_hour[1] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[1] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[1] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[1] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[1] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[1] == -1)
        		{
        			ui_day_view_max_hour[1] = ui_day_view_min_hour[1] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[1] < dis_hr)
        			{
        				ui_day_view_max_hour[1] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[1] > dis_hr)
        			{
        				ui_day_view_min_hour[1] = dis_hr;
        			}
        		}
    			ui_day_view_count[1] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[1] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[1] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[1] == -1)
        		{
        			ui_day_buy_max_hour[1] = ui_day_buy_min_hour[1] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[1] < dis_hr)
        			{
        				ui_day_buy_max_hour[1] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[1] > dis_hr)
        			{
        				ui_day_buy_min_hour[1] = dis_hr;
        			}
        		}
    			ui_day_buy_count[1] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[1] += 1;
    	}
    	//d3
    	if (dt_m == 12 && dt_d > 14 + offsize && dt_d < 16 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[2] == -1)
    		{
    			ui_day_bahavior_max_hour[2] = ui_day_bahavior_min_hour[2] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[2] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[2] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[2] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[2] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[2] == -1)
        		{
        			ui_day_view_max_hour[2] = ui_day_view_min_hour[2] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[2] < dis_hr)
        			{
        				ui_day_view_max_hour[2] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[2] > dis_hr)
        			{
        				ui_day_view_min_hour[2] = dis_hr;
        			}
        		}
    			ui_day_view_count[2] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[2] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[2] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[2] == -1)
        		{
        			ui_day_buy_max_hour[2] = ui_day_buy_min_hour[2] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[2] < dis_hr)
        			{
        				ui_day_buy_max_hour[2] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[2] > dis_hr)
        			{
        				ui_day_buy_min_hour[2] = dis_hr;
        			}
        		}
    			ui_day_buy_count[2] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[2] += 1;
    	}
    	//d4
    	if (dt_m == 12 && dt_d > 13 + offsize && dt_d < 15 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[3] == -1)
    		{
    			ui_day_bahavior_max_hour[3] = ui_day_bahavior_min_hour[3] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[3] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[3] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[3] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[3] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[3] == -1)
        		{
        			ui_day_view_max_hour[3] = ui_day_view_min_hour[3] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[3] < dis_hr)
        			{
        				ui_day_view_max_hour[3] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[3] > dis_hr)
        			{
        				ui_day_view_min_hour[3] = dis_hr;
        			}
        		}
    			ui_day_view_count[3] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[3] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[3] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[3] == -1)
        		{
        			ui_day_buy_max_hour[3] = ui_day_buy_min_hour[3] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[3] < dis_hr)
        			{
        				ui_day_buy_max_hour[3] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[3] > dis_hr)
        			{
        				ui_day_buy_min_hour[3] = dis_hr;
        			}
        		}
    			ui_day_buy_count[3] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[3] += 1;
    	}
    	//d5
    	if (dt_m == 12 && dt_d > 12 + offsize && dt_d < 14 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[4] == -1)
    		{
    			ui_day_bahavior_max_hour[4] = ui_day_bahavior_min_hour[4] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[4] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[4] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[4] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[4] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[4] == -1)
        		{
        			ui_day_view_max_hour[4] = ui_day_view_min_hour[4] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[4] < dis_hr)
        			{
        				ui_day_view_max_hour[4] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[4] > dis_hr)
        			{
        				ui_day_view_min_hour[4] = dis_hr;
        			}
        		}
    			ui_day_view_count[4] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[4] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[4] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[4] == -1)
        		{
        			ui_day_buy_max_hour[4] = ui_day_buy_min_hour[4] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[4] < dis_hr)
        			{
        				ui_day_buy_max_hour[4] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[4] > dis_hr)
        			{
        				ui_day_buy_min_hour[4] = dis_hr;
        			}
        		}
    			ui_day_buy_count[4] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[4] += 1;
    	}
    	//d6
    	if (dt_m == 12 && dt_d > 11 + offsize && dt_d < 13 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[5] == -1)
    		{
    			ui_day_bahavior_max_hour[5] = ui_day_bahavior_min_hour[5] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[5] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[5] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[5] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[5] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[5] == -1)
        		{
        			ui_day_view_max_hour[5] = ui_day_view_min_hour[5] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[5] < dis_hr)
        			{
        				ui_day_view_max_hour[5] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[5] > dis_hr)
        			{
        				ui_day_view_min_hour[5] = dis_hr;
        			}
        		}
    			ui_day_view_count[5] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[5] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[5] += 1;
    			break;
    		case 5:
    			if (ui_day_buy_max_hour[5] == -1)
        		{
        			ui_day_buy_max_hour[5] = ui_day_buy_min_hour[5] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[5] < dis_hr)
        			{
        				ui_day_buy_max_hour[5] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[5] > dis_hr)
        			{
        				ui_day_buy_min_hour[5] = dis_hr;
        			}
        		}
    			ui_day_buy_count[5] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[5] += 1;
    	}
    	//d7
    	if (dt_m == 12 && dt_d > 10 + offsize && dt_d < 12 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[6] == -1)
    		{
    			ui_day_bahavior_max_hour[6] = ui_day_bahavior_min_hour[6] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[6] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[6] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[6] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[6] = dis_hr;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[6] == -1)
        		{
        			ui_day_view_max_hour[6] = ui_day_view_min_hour[6] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[6] < dis_hr)
        			{
        				ui_day_view_max_hour[6] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[6] > dis_hr)
        			{
        				ui_day_view_min_hour[6] = dis_hr;
        			}
        		}
    			ui_day_view_count[6] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[6] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[6] += 1;
    			break;
    		case 6:
    			if (ui_day_buy_max_hour[6] == -1)
        		{
        			ui_day_buy_max_hour[6] = ui_day_buy_min_hour[6] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[6] < dis_hr)
        			{
        				ui_day_buy_max_hour[6] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[6] > dis_hr)
        			{
        				ui_day_buy_min_hour[6] = dis_hr;
        			}
        		}
    			ui_day_buy_count[6] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[6] += 1;
    	}

    }
   
    
    public void genFeature1(int tp, int hr, int dt_m, int dt_d)
    {
    	int dis_hr = (17 + offsize - dt_d) * 24 + (24 - hr);
    	int dis_dt_d = 18 + offsize - dt_d;
    	//d1-d3
    	if (dt_m == 12 && dt_d > 14 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[7] == -1)
    		{
    			ui_day_bahavior_max_hour[7] = ui_day_bahavior_min_hour[7] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[7] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[7] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[7] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[7] = dis_hr;
    			}
    		}
    		//date
    		if (ui_day_bahavior_max_dt[7] == -1)
    		{
    			ui_day_bahavior_max_dt[7] = ui_day_bahavior_min_dt[7] = dt_d;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_dt[7] < dt_d)
    			{
    				ui_day_bahavior_max_dt[7] = dt_d;
    			}
    			else if (ui_day_bahavior_min_dt[7] > dt_d)
    			{
    				ui_day_bahavior_min_dt[7] = dt_d;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[7] == -1)
        		{
        			ui_day_view_max_hour[7] = ui_day_view_min_hour[7] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[7] < dis_hr)
        			{
        				ui_day_view_max_hour[7] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[7] > dis_hr)
        			{
        				ui_day_view_min_hour[7] = dis_hr;
        			}
        		}
    			//date
        		if (ui_day_view_max_dt[7] == -1)
        		{
        			ui_day_view_max_dt[7] = ui_day_view_min_dt[7] = dt_d;
        		}
        		else 
        		{
        			if (ui_day_view_max_dt[7] < dt_d)
        			{
        				ui_day_view_max_dt[7] = dt_d;
        			}
        			else if (ui_day_view_min_dt[7] > dt_d)
        			{
        				ui_day_view_min_dt[7] = dt_d;
        			}
        		}
    			ui_day_view_count[7] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[7] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[7] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[7] == -1)
        		{
        			ui_day_buy_max_hour[7] = ui_day_buy_min_hour[7] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[7] < dis_hr)
        			{
        				ui_day_buy_max_hour[7] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[7] > dis_hr)
        			{
        				ui_day_buy_min_hour[7] = dis_hr;
        			}
        		}
    			//date
        		if (ui_day_buy_max_dt[7] == -1)
        		{
        			ui_day_buy_max_dt[7] = ui_day_buy_min_dt[7] = dt_d;
        		}
        		else 
        		{
        			if (ui_day_buy_max_dt[7] < dt_d)
        			{
        				ui_day_buy_max_dt[7] = dt_d;
        			}
        			else if (ui_day_buy_min_dt[7] > dt_d)
        			{
        				ui_day_buy_min_dt[7] = dt_d;
        			}
        		}
    			ui_day_buy_count[7] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[7] += 1;
    	}
    	//d1-d7
    	if (dt_m == 12 && dt_d > 10 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[8] == -1)
    		{
    			ui_day_bahavior_max_hour[8] = ui_day_bahavior_min_hour[8] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[8] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[8] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[8] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[8] = dis_hr;
    			}
    		}
    		//date
    		if (ui_day_bahavior_max_dt[8] == -1)
    		{
    			ui_day_bahavior_max_dt[8] = ui_day_bahavior_min_dt[8] = dt_d;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_dt[8] < dt_d)
    			{
    				ui_day_bahavior_max_dt[8] = dt_d;
    			}
    			else if (ui_day_bahavior_min_dt[8] > dt_d)
    			{
    				ui_day_bahavior_min_dt[8] = dt_d;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[8] == -1)
        		{
        			ui_day_view_max_hour[8] = ui_day_view_min_hour[8] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[8] < dis_hr)
        			{
        				ui_day_view_max_hour[8] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[8] > dis_hr)
        			{
        				ui_day_view_min_hour[8] = dis_hr;
        			}
        		}
    			//date
        		if (ui_day_view_max_dt[8] == -1)
        		{
        			ui_day_view_max_dt[8] = ui_day_view_min_dt[8] = dt_d;
        		}
        		else 
        		{
        			if (ui_day_view_max_dt[8] < dt_d)
        			{
        				ui_day_view_max_dt[8] = dt_d;
        			}
        			else if (ui_day_view_min_dt[8] > dt_d)
        			{
        				ui_day_view_min_dt[8] = dt_d;
        			}
        		}
    			ui_day_view_count[8] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[8] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[8] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[8] == -1)
        		{
        			ui_day_buy_max_hour[8] = ui_day_buy_min_hour[8] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[8] < dis_hr)
        			{
        				ui_day_buy_max_hour[8] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[8] > dis_hr)
        			{
        				ui_day_buy_min_hour[8] = dis_hr;
        			}
        		}
    			//date
        		if (ui_day_buy_max_dt[8] == -1)
        		{
        			ui_day_buy_max_dt[8] = ui_day_buy_min_dt[8] = dt_d;
        		}
        		else 
        		{
        			if (ui_day_buy_max_dt[8] < dt_d)
        			{
        				ui_day_buy_max_dt[8] = dt_d;
        			}
        			else if (ui_day_buy_min_dt[8] > dt_d)
        			{
        				ui_day_buy_min_dt[8] = dt_d;
        			}
        		}
    			ui_day_buy_count[8] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[8] += 1;
    	}
    	//d1-d14
    	if (dt_m == 12 && dt_d > 3 + offsize && dt_d < 18 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[9] == -1)
    		{
    			ui_day_bahavior_max_hour[9] = ui_day_bahavior_min_hour[9] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[9] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[9] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[9] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[9] = dis_hr;
    			}
    		}
    		//date
    		if (ui_day_bahavior_max_dt[9] == -1)
    		{
    			ui_day_bahavior_max_dt[9] = ui_day_bahavior_min_dt[9] = dt_d;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_dt[9] < dt_d)
    			{
    				ui_day_bahavior_max_dt[9] = dt_d;
    			}
    			else if (ui_day_bahavior_min_dt[9] > dt_d)
    			{
    				ui_day_bahavior_min_dt[9] = dt_d;
    			}
    		}
    		switch(tp)
    		{
    		case 1:
    			if (ui_day_view_max_hour[9] == -1)
        		{
        			ui_day_view_max_hour[9] = ui_day_view_min_hour[9] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_view_max_hour[9] < dis_hr)
        			{
        				ui_day_view_max_hour[9] = dis_hr;
        			}
        			else if (ui_day_view_min_hour[9] > dis_hr)
        			{
        				ui_day_view_min_hour[9] = dis_hr;
        			}
        		}
    			//date
        		if (ui_day_view_max_dt[9] == -1)
        		{
        			ui_day_view_max_dt[9] = ui_day_view_min_dt[9] = dt_d;
        		}
        		else 
        		{
        			if (ui_day_view_max_dt[9] < dt_d)
        			{
        				ui_day_view_max_dt[9] = dt_d;
        			}
        			else if (ui_day_view_min_dt[9] > dt_d)
        			{
        				ui_day_view_min_dt[9] = dt_d;
        			}
        		}
    			ui_day_view_count[9] += 1;
    			break;
    		case 2:
    			ui_day_collect_count[9] += 1;
    			break;
    		case 3:
    			ui_day_cart_count[9] += 1;
    			break;
    		case 4:
    			if (ui_day_buy_max_hour[9] == -1)
        		{
        			ui_day_buy_max_hour[9] = ui_day_buy_min_hour[9] = dis_hr;
        		}
        		else 
        		{
        			if (ui_day_buy_max_hour[9] < dis_hr)
        			{
        				ui_day_buy_max_hour[9] = dis_hr;
        			}
        			else if (ui_day_buy_min_hour[9] > dis_hr)
        			{
        				ui_day_buy_min_hour[9] = dis_hr;
        			}
        		}
    			//date
        		if (ui_day_buy_max_dt[9] == -1)
        		{
        			ui_day_buy_max_dt[9] = ui_day_buy_min_dt[9] = dt_d;
        		}
        		else 
        		{
        			if (ui_day_buy_max_dt[9] < dt_d)
        			{
        				ui_day_buy_max_dt[9] = dt_d;
        			}
        			else if (ui_day_buy_min_dt[9] > dt_d)
        			{
        				ui_day_buy_min_dt[9] = dt_d;
        			}
        		}
    			ui_day_buy_count[9] += 1;
    			break;
    		default:
    			break;
    		}
    		ui_day_behavior_count[9] += 1;
    	}
    }
   
    public void genFeature2(int tp, int hr, int dt_m, int dt_d)
    {
    	int dis_hr, dis_dt_d;
    	if (dt_m == 12)
    	{
    		dis_hr = (17 + offsize - dt_d) * 24 + (24 - hr);
        	dis_dt_d = 18 + offsize - dt_d;
    	}else
    	{
    		dis_hr = (17 + offsize) * 24 + (30 - dt_d) * 24 + (24 - hr);
        	dis_dt_d = 18 + offsize + 30 - dt_d;
    	}
    	
    	set_ui_behavior_days.add(dis_dt_d);
    	set_ui_behavior_hours.add(dis_hr);
    	
    	//hour
    	if (ui_day_bahavior_max_hour[11] == -1)
    	{
    		ui_day_bahavior_max_hour[11] = ui_day_bahavior_min_hour[11] = dis_hr;
    		ui_last_behavior_type = tp;
    	}
    	else 
    	{
    		if (ui_day_bahavior_max_hour[11] < dis_hr)
    		{
    			ui_day_bahavior_max_hour[11] = dis_hr;
    		}
    		else if (ui_day_bahavior_min_hour[11] > dis_hr)
    		{
    			ui_day_bahavior_min_hour[11] = dis_hr;
    			ui_last_behavior_type = tp;
    		}
    	}
    	//date
    	if (ui_day_bahavior_max_dt[11] == -1)
    	{
    		ui_day_bahavior_max_dt[11] = ui_day_bahavior_min_dt[11] = dt_d;
    	}
    	else 
    	{
    		if (ui_day_bahavior_max_dt[11] < dt_d)
    		{
    			ui_day_bahavior_max_dt[11] = dt_d;
    		}
    		else if (ui_day_bahavior_min_dt[11] > dt_d)
    		{
    			ui_day_bahavior_min_dt[11] = dt_d;
    		}
    	}
    	switch(tp)
    	{
    	case 1:
    		if (ui_day_view_max_hour[11] == -1)
    		{
    			ui_day_view_max_hour[11] = ui_day_view_min_hour[11] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_view_max_hour[11] < dis_hr)
    			{
    				ui_day_view_max_hour[11] = dis_hr;
    			}
    			else if (ui_day_view_min_hour[11] > dis_hr)
    			{
    				ui_day_view_min_hour[11] = dis_hr;
    			}
    		}
    		//date
    		if (ui_day_view_max_dt[11] == -1)
    		{
    			ui_day_view_max_dt[11] = ui_day_view_min_dt[11] = dt_d;
    		}
    		else 
    		{
    			if (ui_day_view_max_dt[11] < dt_d)
    			{
    				ui_day_view_max_dt[11] = dt_d;
    			}
    			else if (ui_day_view_min_dt[11] > dt_d)
    			{
    				ui_day_view_min_dt[11] = dt_d;
    			}
    		}
    		ui_day_view_count[11] += 1;
    		break;
    	case 2:
    		ui_day_collect_count[11] += 1;
    		break;
    	case 3:
    		ui_day_cart_count[11] += 1;
    		break;
    	case 4:
    		if (ui_day_buy_max_hour[11] == -1)
    		{
    			ui_day_buy_max_hour[11] = ui_day_buy_min_hour[11] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_buy_max_hour[11] < dis_hr)
    			{
    				ui_day_buy_max_hour[11] = dis_hr;
    			}
    			else if (ui_day_buy_min_hour[11] > dis_hr)
    			{
    				ui_day_buy_min_hour[11] = dis_hr;
    			}
    		}
    		//date
    		if (ui_day_buy_max_dt[11] == -1)
    		{
    			ui_day_buy_max_dt[11] = ui_day_buy_min_dt[11] = dt_d;
    		}
    		else 
    		{
    			if (ui_day_buy_max_dt[11] < dt_d)
    			{
    				ui_day_buy_max_dt[11] = dt_d;
    			}
    			else if (ui_day_buy_min_dt[11] > dt_d)
    			{
    				ui_day_buy_min_dt[11] = dt_d;
    			}
    		}
    		ui_day_buy_count[11] += 1;
    		break;
    	default:
    		break;
    	}
    	ui_day_behavior_count[11] += 1;
    	
    	//d8-d14
    	if (dt_m == 12 && dt_d > 3 + offsize && dt_d < 11 + offsize)
    	{
    		//hour
    		if (ui_day_bahavior_max_hour[10] == -1)
    		{
    			ui_day_bahavior_max_hour[10] = ui_day_bahavior_min_hour[10] = dis_hr;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_hour[10] < dis_hr)
    			{
    				ui_day_bahavior_max_hour[10] = dis_hr;
    			}
    			else if (ui_day_bahavior_min_hour[10] > dis_hr)
    			{
    				ui_day_bahavior_min_hour[10] = dis_hr;
    			}
    		}
    		//date
    		if (ui_day_bahavior_max_dt[10] == -1)
    		{
    			ui_day_bahavior_max_dt[10] = ui_day_bahavior_min_dt[10] = dt_d;
    		}
    		else 
    		{
    			if (ui_day_bahavior_max_dt[10] < dt_d)
    			{
    				ui_day_bahavior_max_dt[10] = dt_d;
    			}
    			else if (ui_day_bahavior_min_dt[10] > dt_d)
    			{
    				ui_day_bahavior_min_dt[10] = dt_d;
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
    		lm_makingFeatures(tp, (int)timeDistance, hr);
            
        }
        
       
        
        
        int i_f[] = new int[200];
        //double d_f[] = new double[150]; 
        //d1
        i_f[0] = ui_day1_behavior_cross_hours = (ui_day_bahavior_max_hour[0] == -1) ? 0 : ui_day_bahavior_max_hour[0] - ui_day_bahavior_min_hour[0] + 1;   
        i_f[1] = ui_day1_view_cross_hours = (ui_day_view_max_hour[0] == -1) ? 0 : ui_day_view_max_hour[0] - ui_day_view_min_hour[0] + 1; 
        i_f[2] = ui_day1_buy_cross_hours = (ui_day_buy_max_hour[0] == -1) ? 0 : ui_day_buy_max_hour[0] - ui_day_buy_min_hour[0] + 1;
        i_f[3] = ui_day1_view_count = ui_day_view_count[0];
        i_f[4] = ui_day1_collect_count = ui_day_collect_count[0];
        i_f[5] = ui_day1_cart_count = ui_day_cart_count[0];
        i_f[6] = ui_day1_buy_count = ui_day_buy_count[0];
        i_f[7] = ui_day1_behavior_count = ui_day_behavior_count[0];
        i_f[8] = ui_day1_cart_and_behavior_count = ui_day1_cart_count * ui_day1_behavior_count;//??????????????
        i_f[9] = ui_day1_is_cart = (ui_day1_cart_count > 0) ? 1:0;
        i_f[10] = ui_day1_is_buy = (ui_day1_buy_count > 0) ? 1:0;
        i_f[11] = ui_day1_is_cart_not_buy = (ui_day1_is_cart == 1 && ui_day1_is_buy == 0) ? 1:0;
        //d2
        i_f[12] = ui_day_2_behavior_cross_hours = (ui_day_bahavior_max_hour[1] == -1) ? 0 : ui_day_bahavior_max_hour[1] - ui_day_bahavior_min_hour[1] + 1;   
        i_f[13] = ui_day_2_view_cross_hours = (ui_day_view_max_hour[1] == -1) ? 0 : ui_day_view_max_hour[1] - ui_day_view_min_hour[1] + 1; 
        i_f[14] = ui_day_2_buy_cross_hours = (ui_day_buy_max_hour[1] == -1) ? 0 : ui_day_buy_max_hour[1] - ui_day_buy_min_hour[1] + 1;
        i_f[15] = ui_day_2_view_count = ui_day_view_count[1];
        i_f[16] = ui_day_2_collect_count = ui_day_collect_count[1];
        i_f[17] = ui_day_2_cart_count = ui_day_cart_count[1];
        i_f[18] = ui_day_2_buy_count = ui_day_buy_count[1];
        i_f[19] = ui_day_2_behavior_count = ui_day_behavior_count[1];
        i_f[20] = ui_day_2_cart_and_behavior_count = ui_day_2_cart_count * ui_day_2_behavior_count;//??????????????
        i_f[21] = ui_day_2_is_cart = (ui_day_2_cart_count > 0) ? 1:0;
        i_f[22] = ui_day_2_is_buy = (ui_day_2_buy_count > 0) ? 1:0;
        i_f[23] = ui_day_2_is_cart_not_buy = (ui_day_2_is_cart == 1 && ui_day_2_is_buy == 0) ? 1:0;
        //d3
        i_f[24] = ui_day_3_behavior_cross_hours = (ui_day_bahavior_max_hour[2] == -1) ? 0 : ui_day_bahavior_max_hour[2] - ui_day_bahavior_min_hour[2] + 1;   
        i_f[25] = ui_day_3_view_cross_hours = (ui_day_view_max_hour[2] == -1) ? 0 : ui_day_view_max_hour[2] - ui_day_view_min_hour[2] + 1; 
        i_f[26] = ui_day_3_buy_cross_hours = (ui_day_buy_max_hour[2] == -1) ? 0 : ui_day_buy_max_hour[2] - ui_day_buy_min_hour[2] + 1;
        i_f[27] = ui_day_3_view_count = ui_day_view_count[2];
        i_f[28] = ui_day_3_collect_count = ui_day_collect_count[2];
        i_f[29] = ui_day_3_cart_count = ui_day_cart_count[2];
        i_f[30] = ui_day_3_buy_count = ui_day_buy_count[2];
        i_f[31] = ui_day_3_behavior_count = ui_day_behavior_count[2];
        i_f[32] = ui_day_3_cart_and_behavior_count = ui_day_3_cart_count * ui_day_3_behavior_count;//??????????????
        i_f[33] = ui_day_3_is_cart = (ui_day_3_cart_count > 0) ? 1:0;
        i_f[34] = ui_day_3_is_buy = (ui_day_3_buy_count > 0) ? 1:0;
        i_f[35] = ui_day_3_is_cart_not_buy = (ui_day_3_is_cart == 1 && ui_day_3_is_buy == 0) ? 1:0;
        //d4
        i_f[36] = ui_day_4_behavior_cross_hours = (ui_day_bahavior_max_hour[3] == -1) ? 0 : ui_day_bahavior_max_hour[3] - ui_day_bahavior_min_hour[3] + 1;   
        i_f[37] = ui_day_4_view_cross_hours = (ui_day_view_max_hour[3] == -1) ? 0 : ui_day_view_max_hour[3] - ui_day_view_min_hour[3] + 1; 
        i_f[38] = ui_day_4_buy_cross_hours = (ui_day_buy_max_hour[3] == -1) ? 0 : ui_day_buy_max_hour[3] - ui_day_buy_min_hour[3] + 1;
        i_f[39] = ui_day_4_view_count = ui_day_view_count[3];
        i_f[40] = ui_day_4_collect_count = ui_day_collect_count[3];
        i_f[41] = ui_day_4_cart_count = ui_day_cart_count[3];
        i_f[42] = ui_day_4_buy_count = ui_day_buy_count[3];
        i_f[43] = ui_day_4_behavior_count = ui_day_behavior_count[3];
        i_f[44] = ui_day_4_cart_and_behavior_count = ui_day_4_cart_count * ui_day_4_behavior_count;//??????????????
        i_f[45] = ui_day_4_is_cart = (ui_day_4_cart_count > 0) ? 1:0;
        i_f[46] = ui_day_4_is_buy = (ui_day_4_buy_count > 0) ? 1:0;
        i_f[47] = ui_day_4_is_cart_not_buy = (ui_day_4_is_cart == 1 && ui_day_4_is_buy == 0) ? 1:0;
        //d5
        i_f[48] = ui_day_5_behavior_cross_hours = (ui_day_bahavior_max_hour[4] == -1) ? 0 : ui_day_bahavior_max_hour[4] - ui_day_bahavior_min_hour[4] + 1;   
        i_f[49] = ui_day_5_view_cross_hours = (ui_day_view_max_hour[4] == -1) ? 0 : ui_day_view_max_hour[4] - ui_day_view_min_hour[4] + 1; 
        i_f[50] = ui_day_5_buy_cross_hours = (ui_day_buy_max_hour[4] == -1) ? 0 : ui_day_buy_max_hour[4] - ui_day_buy_min_hour[4] + 1;
        i_f[51] = ui_day_5_view_count = ui_day_view_count[4];
        i_f[52] = ui_day_5_collect_count = ui_day_collect_count[4];
        i_f[53] = ui_day_5_cart_count = ui_day_cart_count[4];
        i_f[54] = ui_day_5_buy_count = ui_day_buy_count[4];
        i_f[55] = ui_day_5_behavior_count = ui_day_behavior_count[4];
        i_f[56] = ui_day_5_cart_and_behavior_count = ui_day_5_cart_count * ui_day_5_behavior_count;//??????????????
        i_f[57] = ui_day_5_is_cart = (ui_day_5_cart_count > 0) ? 1:0;
        i_f[58] = ui_day_5_is_buy = (ui_day_5_buy_count > 0) ? 1:0;
        i_f[59] = ui_day_5_is_cart_not_buy = (ui_day_5_is_cart == 1 && ui_day_5_is_buy == 0) ? 1:0;
        //d6
        i_f[60] = ui_day_6_behavior_cross_hours = (ui_day_bahavior_max_hour[5] == -1) ? 0 : ui_day_bahavior_max_hour[5] - ui_day_bahavior_min_hour[5] + 1;   
        i_f[61] = ui_day_6_view_cross_hours = (ui_day_view_max_hour[5] == -1) ? 0 : ui_day_view_max_hour[5] - ui_day_view_min_hour[5] + 1; 
        i_f[62] = ui_day_6_buy_cross_hours = (ui_day_buy_max_hour[5] == -1) ? 0 : ui_day_buy_max_hour[5] - ui_day_buy_min_hour[5] + 1;
        i_f[63] = ui_day_6_view_count = ui_day_view_count[5];
        i_f[64] = ui_day_6_collect_count = ui_day_collect_count[5];
        i_f[65] = ui_day_6_cart_count = ui_day_cart_count[5];
        i_f[66] = ui_day_6_buy_count = ui_day_buy_count[5];
        i_f[67] = ui_day_6_behavior_count = ui_day_behavior_count[5];
        i_f[68] = ui_day_6_cart_and_behavior_count = ui_day_6_cart_count * ui_day_6_behavior_count;//??????????????
        i_f[69] = ui_day_6_is_cart = (ui_day_6_cart_count > 0) ? 1:0;
        i_f[70] = ui_day_6_is_buy = (ui_day_6_buy_count > 0) ? 1:0;
        i_f[71] = ui_day_6_is_cart_not_buy = (ui_day_6_is_cart == 1 && ui_day_6_is_buy == 0) ? 1:0;
        //d7
        i_f[72] = ui_day_7_behavior_cross_hours = (ui_day_bahavior_max_hour[6] == -1) ? 0 : ui_day_bahavior_max_hour[6] - ui_day_bahavior_min_hour[6] + 1;   
        i_f[73] = ui_day_7_view_cross_hours = (ui_day_view_max_hour[6] == -1) ? 0 : ui_day_view_max_hour[6] - ui_day_view_min_hour[6] + 1; 
        i_f[74] = ui_day_7_buy_cross_hours = (ui_day_buy_max_hour[6] == -1) ? 0 : ui_day_buy_max_hour[6] - ui_day_buy_min_hour[6] + 1;
        i_f[75] = ui_day_7_view_count = ui_day_view_count[6];
        i_f[76] = ui_day_7_collect_count = ui_day_collect_count[6];
        i_f[77] = ui_day_7_cart_count = ui_day_cart_count[6];
        i_f[78] = ui_day_7_buy_count = ui_day_buy_count[6];
        i_f[79] = ui_day_7_behavior_count = ui_day_behavior_count[6];
        i_f[80] = ui_day_7_cart_and_behavior_count = ui_day_7_cart_count * ui_day_7_behavior_count;//??????????????
        i_f[81] = ui_day_7_is_cart = (ui_day_7_cart_count > 0) ? 1:0;
        i_f[82] = ui_day_7_is_buy = (ui_day_7_buy_count > 0) ? 1:0;
        i_f[83] = ui_day_7_is_cart_not_buy = (ui_day_7_is_cart == 1 && ui_day_7_is_buy == 0) ? 1:0;
        //d1-d3
        i_f[84] = ui_day_1_3_bahavior_cross_hours = (ui_day_bahavior_max_hour[7] == -1) ? 0 : ui_day_bahavior_max_hour[7] - ui_day_bahavior_min_hour[7] + 1;
        i_f[85] = ui_day_1_3_view_cross_hours = (ui_day_view_max_hour[7] == -1) ? 0 : ui_day_view_max_hour[7] - ui_day_view_min_hour[7] + 1; 
        i_f[86] = ui_day_1_3_buy_cross_hours = (ui_day_buy_max_hour[7] == -1) ? 0 : ui_day_buy_max_hour[7] - ui_day_buy_min_hour[7] + 1;
        i_f[87] = ui_day_1_3_bahavior_cross_days = (ui_day_bahavior_max_dt[7] == -1) ? 0 : ui_day_bahavior_max_dt[7] - ui_day_bahavior_min_dt[7] + 1;
        i_f[88] = ui_day_1_3_view_cross_days = (ui_day_view_max_dt[7] == -1) ? 0 : ui_day_view_max_dt[7] - ui_day_view_min_dt[7] + 1;
        i_f[89] = ui_day_1_3_buy_cross_days = (ui_day_buy_max_dt[7] == -1) ? 0 : ui_day_buy_max_dt[7] - ui_day_buy_min_dt[7] + 1;
        i_f[90] = ui_day_1_3_view_count = ui_day_view_count[7];
        i_f[91] = ui_day_1_3_collect_count = ui_day_collect_count[7];
        i_f[92] = ui_day_1_3_cart_count = ui_day_cart_count[7];
        i_f[93] = ui_day_1_3_buy_count = ui_day_buy_count[7];
        i_f[94] = ui_day_1_3_behavior_count = ui_day_view_count[7] + ui_day_collect_count[7] + ui_day_cart_count[7] + ui_day_buy_count[7];
        //d1-d7
        i_f[95] = ui_day_1_7_bahavior_cross_hours = (ui_day_bahavior_max_hour[8] == -1) ? 0 : ui_day_bahavior_max_hour[8] - ui_day_bahavior_min_hour[8] + 1;
        i_f[96] = ui_day_1_7_view_cross_hours = (ui_day_view_max_hour[8] == -1) ? 0 : ui_day_view_max_hour[8] - ui_day_view_min_hour[8] + 1; 
        i_f[97] = ui_day_1_7_buy_cross_hours = (ui_day_buy_max_hour[8] == -1) ? 0 : ui_day_buy_max_hour[8] - ui_day_buy_min_hour[8] + 1;
        i_f[98] = ui_day_1_7_bahavior_cross_days = (ui_day_bahavior_max_dt[8] == -1) ? 0 : ui_day_bahavior_max_dt[8] - ui_day_bahavior_min_dt[8] + 1;
        i_f[99] = ui_day_1_7_view_cross_days = (ui_day_view_max_dt[8] == -1) ? 0 : ui_day_view_max_dt[8] - ui_day_view_min_dt[8] + 1;
        i_f[100] = ui_day_1_7_buy_cross_days = (ui_day_buy_max_dt[8] == -1) ? 0 : ui_day_buy_max_dt[8] - ui_day_buy_min_dt[8] + 1;
        i_f[101] = ui_day_1_7_view_count = ui_day_view_count[8];
        i_f[102] = ui_day_1_7_collect_count = ui_day_collect_count[8];
        i_f[103] = ui_day_1_7_cart_count = ui_day_cart_count[8];
        i_f[104] = ui_day_1_7_buy_count = ui_day_buy_count[8];
        i_f[105] = ui_day_1_7_behavior_count = ui_day_view_count[8] + ui_day_collect_count[8] + ui_day_cart_count[8] + ui_day_buy_count[8];
        //d1-d14
        i_f[106] = ui_day_1_14_bahavior_cross_hours = (ui_day_bahavior_max_hour[9] == -1) ? 0 : ui_day_bahavior_max_hour[9] - ui_day_bahavior_min_hour[9] + 1;
        i_f[107] = ui_day_1_14_view_cross_hours = (ui_day_view_max_hour[9] == -1) ? 0 : ui_day_view_max_hour[9] - ui_day_view_min_hour[9] + 1; 
        i_f[108] = ui_day_1_14_buy_cross_hours = (ui_day_buy_max_hour[9] == -1) ? 0 : ui_day_buy_max_hour[9] - ui_day_buy_min_hour[9] + 1;
        i_f[109] = ui_day_1_14_bahavior_cross_days = (ui_day_bahavior_max_dt[9] == -1) ? 0 : ui_day_bahavior_max_dt[9] - ui_day_bahavior_min_dt[9] + 1;
        i_f[110] = ui_day_1_14_view_cross_days = (ui_day_view_max_dt[9] == -1) ? 0 : ui_day_view_max_dt[9] - ui_day_view_min_dt[9] + 1;
        i_f[111] = ui_day_1_14_buy_cross_days = (ui_day_buy_max_dt[9] == -1) ? 0 : ui_day_buy_max_dt[9] - ui_day_buy_min_dt[9] + 1;
        i_f[112] = ui_day_1_14_view_count = ui_day_view_count[9];
        i_f[113] = ui_day_1_14_collect_count = ui_day_collect_count[9];
        i_f[114] = ui_day_1_14_cart_count = ui_day_cart_count[9];
        i_f[115] = ui_day_1_14_buy_count = ui_day_buy_count[9];
        i_f[116] = ui_day_1_14_behavior_count = ui_day_view_count[9] + ui_day_collect_count[9] + ui_day_cart_count[9] + ui_day_buy_count[9];
        //d2-d3
        i_f[117] = ui_day_2_3_view_count = ui_day_2_view_count + ui_day_3_view_count;
        i_f[118] = ui_day_2_3_collect_count = ui_day_2_collect_count + ui_day_3_collect_count;
        i_f[119] = ui_day_2_3_cart_count = ui_day_2_cart_count + ui_day_3_cart_count;
        i_f[120] = ui_day_2_3_buy_count = ui_day_2_buy_count + ui_day_3_buy_count;
        i_f[121] = ui_day_2_3_behavior_count = ui_day_2_3_view_count + ui_day_2_3_collect_count + ui_day_2_3_cart_count + ui_day_2_3_buy_count;
        //d4-d7
        i_f[122] = ui_day_4_7_view_count = ui_day_4_view_count + ui_day_5_view_count + ui_day_6_view_count +ui_day_7_view_count;
        i_f[123] = ui_day_4_7_collect_count = ui_day_4_collect_count + ui_day_5_collect_count + ui_day_6_collect_count +ui_day_7_collect_count;
        i_f[124] = ui_day_4_7_cart_count = ui_day_4_cart_count + ui_day_5_cart_count + ui_day_6_cart_count +ui_day_7_cart_count;
        i_f[125] = ui_day_4_7_buy_count = ui_day_4_buy_count + ui_day_5_buy_count + ui_day_6_buy_count +ui_day_7_buy_count;
        i_f[126] = ui_day_4_7_behavior_count = ui_day_4_7_view_count + ui_day_4_7_collect_count + ui_day_4_7_cart_count + ui_day_4_7_buy_count;
        //d8-d14
        i_f[127] = ui_day_8_14_view_count = ui_day_view_count[9] - ui_day_view_count[8];
        i_f[128] = ui_day_8_14_collect_count = ui_day_collect_count[9] - ui_day_collect_count[8];
        i_f[129] = ui_day_8_14_cart_count = ui_day_cart_count[9] - ui_day_cart_count[8];
        i_f[130] = ui_day_8_14_buy_count = ui_day_buy_count[9] - ui_day_buy_count[8];
        i_f[131] = ui_day_8_14_behavior_count = ui_day_behavior_count[9] - ui_day_behavior_count[8];
        i_f[132] = ui_day_8_14_behavior_cross_hours = (ui_day_bahavior_max_hour[10] == -1) ? 0 : ui_day_bahavior_max_hour[10] - ui_day_bahavior_min_hour[10] + 1;
        i_f[133] = ui_day_8_14_behavior_cross_days = (ui_day_bahavior_max_dt[10] == -1) ? 0 : ui_day_bahavior_max_dt[10] - ui_day_bahavior_min_dt[10] + 1;
        i_f[134] = ui_day_8_14_is_cart = (ui_day_8_14_cart_count > 0) ? 1:0;
        i_f[135] = ui_day_8_14_is_buy = (ui_day_8_14_buy_count > 0) ? 1:0;
        i_f[136] = ui_day_8_14_is_cart_not_buy = (ui_day_8_14_cart_count == 1 && ui_day_8_14_buy_count == 0) ? 1:0;
        
        i_f[137] = ui_last_behavior_dis_hours = ui_day_bahavior_min_hour[11];
        i_f[138] = ui_last_behavior_dis_days = 18 + offsize - ui_day_bahavior_min_dt[11];
        i_f[139] = ui_last_behavior_type;
        i_f[140] = ui_last_view_dis_hours = ui_day_view_min_hour[11]; 
        i_f[141] = ui_last_buy_dis_hours = (ui_day_buy_min_hour[11] == -1) ? 720:ui_day_buy_min_hour[11]; 
        
        i_f[142] = ui_all_view_count = ui_day_view_count[11];
        i_f[143] = ui_all_collect_count = ui_day_collect_count[11];
        i_f[144] = ui_all_cart_count = ui_day_cart_count[11];
        i_f[145] = ui_all_buy_count = ui_day_buy_count[11];
        i_f[146] = ui_all_behavior_count = ui_all_view_count + ui_all_collect_count + ui_all_cart_count + ui_all_buy_count;
        
        i_f[147] = ui_is_ever_view = (ui_day_view_count[11] > 0) ? 1:0;
        i_f[148] = ui_is_ever_collect = (ui_day_collect_count[11] > 0) ? 1:0;
        i_f[149] = ui_is_ever_cart = (ui_day_cart_count[11] > 0) ? 1:0;
        i_f[150] = ui_is_ever_buy = (ui_day_buy_count[11] > 0) ? 1:0;
        
        i_f[151] = tui_day2_continuous_buy = (ui_day_buy_count[0] > 0 && ui_day_buy_count[1] > 0) ? 1:0;
        i_f[152] = tui_day3_continuous_buy = (ui_day_buy_count[0] + ui_day_buy_count[1] + ui_day_buy_count[2] > 1) ? 1:0;
        i_f[153] = tui_pre_3_view_1_cart = (ui_day_view_count[7]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[154] = tui_pre_7_view_1_cart = (ui_day_view_count[8]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[155] = tui_pre_3_collect_1_cart = (ui_day_collect_count[7]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[156] = tui_pre_7_collect_1_cart = (ui_day_collect_count[8]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[157] = tui_pre_3_cart_1_cart = (ui_day_cart_count[7]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[158] = tui_pre_7_cart_1_cart = (ui_day_cart_count[8]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[159] = tui_pre_3_buy_1_cart = (ui_day_buy_count[7]>0 && ui_day_cart_count[0]>0) ? 1:0;
        i_f[160] = tui_pre_7_buy_1_cart = (ui_day_cart_count[8]>0 && ui_day_cart_count[0]>0) ? 1:0;  
        i_f[161] = ui_behavior_days = set_ui_behavior_days.size();
        i_f[162] = ui_behavior_hours = set_ui_behavior_hours.size();
        
        i_f[163] = ui_day1_view_cnt[0];
        i_f[164] = ui_day1_view_cnt[1];
        i_f[165] = ui_day1_view_cnt[2];
        i_f[166] = ui_day1_view_cnt[3];
        i_f[167] = ui_day1_collect_cnt[0];
        i_f[168] = ui_day1_collect_cnt[1];
        i_f[169] = ui_day1_collect_cnt[2];
        i_f[170] = ui_day1_collect_cnt[3];
        i_f[171] = ui_day1_cart_cnt[0];
        i_f[172] = ui_day1_cart_cnt[1];
        i_f[173] = ui_day1_cart_cnt[2];
        i_f[174] = ui_day1_cart_cnt[3];
        i_f[175] = ui_day1_buy_cnt[0];
        i_f[176] = ui_day1_buy_cnt[1];
        i_f[177] = ui_day1_buy_cnt[2];
        i_f[178] = ui_day1_buy_cnt[3];
        i_f[179] = ui_day1_behavior_cnt[0];
        i_f[180] = ui_day1_behavior_cnt[1];
        i_f[181] = ui_day1_behavior_cnt[2];
        i_f[182] = ui_day1_behavior_cnt[3];
        
        
        // by lemon 
        if (ui_day_1_14_behavior_count ==0 ) 
        	return  ;
        
        
        
        result.set(0,key.getString("uid"));
        result.set(1,key.getString("iid"));
        result.set(2,key.getString("ic"));
        int counter = 3 ;
        
        
        result.set(counter , ui_day_1_14_behavior_count);
        for (int i = 0; i < 183; i++)
        {
        	result.set(i+3, i_f[i]); counter ++ ;
        }
        
        // by lemon
        lm_getFeatures();
        ArrayList lm_fList = new ArrayList();
        lm_fList.add(ui_ever_buy) ;	 // 1、u对i是否有过购买行为；
        lm_fList.add(ui_bf7_buy_pre3_view) ;	 // 2、u对i在超过7天前有过购买行为,且最近3天内有点击行为；
        lm_fList.add(ui_bf7_buy_pre3_cart);	 // 3、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车；
        lm_fList.add(ui_bf7_buy_pre3_cart_pre3_nobuy) ;	 // 4、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车，且尚未购买；
        lm_fList.add(ui_lately_inter_dtn_hr ) ;	 // 5、u对i最后行为距离预测日的小时数；
        lm_fList.add(ui_lately_cart_dtn_hr) ;	 // 6、u把i加入购物车距离预测日的小时数，未加入过则为24*31；
        lm_fList.add(ui_inter_day ) ;	 // 7、u对i的行为总天数（30天）；
        lm_fList.add(ui_buy_day	 )  ;   // 8、u对i的购买总天数（30天）；
        lm_fList.add(ui_inter_cnt)  ;	 // 9、u对i的行为总次数（30天）；
        lm_fList.add(ui_buy_cnt  )  ;	 // 10、u对i的购买总次数（30天）；
        lm_fList.add(ui_buy_cnt_to_inter_cnt ) ;	 // 11、u对i的购买总次数/u对i的行为总次数（30天）；
        lm_fList.add(ui_buy_day_to_inter_day  ) ;	 // 12、u对i的购买总天数/u对i的行为天次数（30天）；
        lm_fList.add(ui_buy_cnt_to_inter_day  ) ;	 // 13、u对i的购买总次数/u对i的行为天次数（30天）；
        lm_fList.add(ui_max_dtn_inter_hr ) ;	 // 14、u对i行为的最大小时跨度；
        lm_fList.add(ui_max_dtn_inter_hr_log_max_dtn_inter_hr ) ; // 15
        
        for(Iterator it = lm_fList.iterator();it.hasNext();){
            result.set( counter ,it.next());
            counter ++;
       }
        
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
    
    
/**
 * 
 * ***************************************** split line *********************************
 * by lemon    
 *  
 */
    
    private int ui_ever_buy ;	 // 1、u对i是否有过购买行为；
//    private	int ui_bf7_buy_pre7_nobuy_pre3_view;	 // 2、u对i在超过7天前有过购买行为，7天内没发生购买，且最近3天内有点击行为；
    private	int ui_bf7_buy_pre3_view;	 // 2、u对i在超过7天前有过购买行为,且最近3天内有点击行为；
//    private	int ui_bf7_buy_pre7_nobuy_pre3_cart ;	 // 3、u对i在超过7天前有过购买行为，7天内没发生购买，且最近3天内有加入过购物车；
    private	int ui_bf7_buy_pre3_cart ;	 // 3、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车；
    private	int ui_bf7_buy_pre3_cart_pre3_nobuy;	 // 4、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车，且尚未购买；
   
    private	int ui_lately_inter_dtn_hr ;	 // 5、u对i最后行为距离预测日的小时数；
    
    private	int ui_lately_cart_dtn_hr ;	 // 6、u把i加入购物车距离预测日的小时数，未加入过则为24*30；
    
    private	int ui_inter_day ;	 // 7、u对i的行为总天数（30天）；
    private	int ui_buy_day	 ;   // 8、u对i的购买总天数（30天）；
    private	int ui_inter_cnt ;	 // 9、u对i的行为总次数（30天）；
    private	int ui_buy_cnt   ;	 // 10、u对i的购买总次数（30天）；
    private	double ui_buy_cnt_to_inter_cnt ;	 // 11、u对i的购买总次数/u对i的行为总次数（30天）；
    private	double ui_buy_day_to_inter_day ;	 // 12、u对i的购买总天数/u对i的行为天次数（30天）；
    private	double ui_buy_cnt_to_inter_day ;	 // 13、u对i的购买总次数/u对i的行为天次数（30天）；
    private	int ui_max_dtn_inter_hr ;	 // 14、u对i行为的最大小时跨度；
    private	double	ui_max_dtn_inter_hr_log_max_dtn_inter_hr ; // 15、u对i最后行为距离预测天小时数 * log (u对i行为的最大小时跨度);
    
    private int bf7_ever_buy ;  // 7 天之前（不包括距离预测日的第7天）有购买过
    private int[][] tpDistributionInDate ; // 各种行为在31 天上的分布
    private boolean[] interDistributionInHour ;
    
    public void lm_init(){
      ui_ever_buy = 0 ;	 // 1、u对i是否有过购买行为；
//      private	int ui_bf7_buy_pre7_nobuy_pre3_view;	 // 2、u对i在超过7天前有过购买行为，7天内没发生购买，且最近3天内有点击行为；
      ui_bf7_buy_pre3_view = 0 ;	 // 2、u对i在超过7天前有过购买行为,且最近3天内有点击行为；
//      private	int ui_bf7_buy_pre7_nobuy_pre3_cart ;	 // 3、u对i在超过7天前有过购买行为，7天内没发生购买，且最近3天内有加入过购物车；
      ui_bf7_buy_pre3_cart  = 0 ;	 // 3、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车；
      ui_bf7_buy_pre3_cart_pre3_nobuy = 0 ;	 // 4、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车，且尚未购买；
     
      ui_lately_inter_dtn_hr  = 31*24 ;	 // 5、u对i最后行为距离预测日的小时数；
      
      ui_lately_cart_dtn_hr = 24*31 ;	 // 6、u把i加入购物车距离预测日的小时数，未加入过则为24*31；
      
      ui_inter_day  = 0 ;	 // 7、u对i的行为总天数（30天）；
      ui_buy_day	 = 0  ;   // 8、u对i的购买总天数（30天）；
      ui_inter_cnt = 0  ;	 // 9、u对i的行为总次数（30天）；
      ui_buy_cnt   = 0  ;	 // 10、u对i的购买总次数（30天）；
      ui_buy_cnt_to_inter_cnt  = 0. ;	 // 11、u对i的购买总次数/u对i的行为总次数（30天）；
      ui_buy_day_to_inter_day  = 0. ;	 // 12、u对i的购买总天数/u对i的行为天次数（30天）；
      ui_buy_cnt_to_inter_day  = 0. ;	 // 13、u对i的购买总次数/u对i的行为天次数（30天）；
      ui_max_dtn_inter_hr  = 0 ;	 // 14、u对i行为的最大小时跨度；
      ui_max_dtn_inter_hr_log_max_dtn_inter_hr = 0.  ; // 15、u对i最后行为距离预测天小时数 * log (u对i行为的最大小时跨度);
    
      tpDistributionInDate = new int [4][31] ; // 每种行为在时间上的分布
      interDistributionInHour = new  boolean[31*24] ;  //  交互在 小时粒度上的bool 交互
    }
    
    // by lemon 
    public void lm_makingFeatures(int tp , int timeDistance , int hr  ){
    	int d = timeDistance * 24 - hr ;
    	tpDistributionInDate[tp-1][timeDistance-1] += 1 ;
    	interDistributionInHour[d] = true ; 
    	
    	if ( tp == 4 && ui_ever_buy == 0) ui_ever_buy =1 ; // 1、u对i是否有过购买行为；
    	if ( bf7_ever_buy == 0 && tp == 4 && timeDistance >7 ) bf7_ever_buy =1 ;
		
    	if ( d < ui_lately_inter_dtn_hr ) ui_lately_inter_dtn_hr = d ;// 5、u对i最后行为距离预测日的小时数；
    	
    	if ( d < ui_lately_cart_dtn_hr && tp ==3 ) ui_lately_cart_dtn_hr =d ; // 6、u把i加入购物车距离预测日的小时数，未加入过则为24*30；
      
    }
    public void lm_getFeatures(){
    	if ( bf7_ever_buy ==1 && ui_day_1_3_view_count > 0 ) ui_bf7_buy_pre3_view = 1 ;	 // 2、u对i在超过7天前有过购买行为,且最近3天内有点击行为；
    	if ( bf7_ever_buy ==1 && ui_day_1_3_cart_count > 0 ) ui_bf7_buy_pre3_cart = 1 ;	 // 3、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车；
    	if ( ui_bf7_buy_pre3_cart ==1 && ui_day_1_3_buy_count == 0 ) ui_bf7_buy_pre3_cart_pre3_nobuy = 0 ;	 // 4、u对i在超过7天前有过购买行为，且最近3天内有加入过购物车，且尚未购买；
    	
    	int[] distrib_day = new int[30 ] ;
    	for( int i =0 ; i <30; i ++ ){
    		distrib_day[i] = tpDistributionInDate[0][i] + tpDistributionInDate[1][i] + tpDistributionInDate[2][i] + tpDistributionInDate[3][i] ;
    		if ( distrib_day[i] >0  ){ ui_inter_day ++ ;} // 7、u对i的行为总天数（30天）；
    	}
    	
    	for( int i =0 ; i <30; i ++ ){
    		if (tpDistributionInDate[3][i] >0  ){ ui_buy_day ++ ;}  // 8、u对i的购买总天数（30天）；
    	}
    	
    	for (int i = 0;i <30;i++ ){ 
    		ui_inter_cnt += distrib_day[i] ; // 9、u对i的行为总次数（30天）；
    		ui_buy_cnt   +=  tpDistributionInDate[3][i]  ;	 // 10、u对i的购买总次数（30天）；
    	}
        ui_buy_cnt_to_inter_cnt  = ui_buy_cnt*1.0 / ui_inter_cnt ;	 // 11、u对i的购买总次数/u对i的行为总次数（30天）；
        ui_buy_day_to_inter_day  = ui_buy_day *1.0 / ui_inter_day ;	 // 12、u对i的购买总天数/u对i的行为天次数（30天）；
        ui_buy_cnt_to_inter_day  = ui_buy_cnt*1.0 / ui_inter_day ;	 // 13、u对i的购买总次数/u对i的行为天次数（30天）；
        
        ArrayList<Integer> interBoolIndex = new ArrayList<Integer>() ;
        for(int i =0 ; i< 31*24;i++){
        	if( interDistributionInHour[i] == true ) interBoolIndex.add(i);
        }
        ui_max_dtn_inter_hr  = interBoolIndex.get( interBoolIndex.size()-1) -  interBoolIndex.get(0) ;	 // 14、u对i行为的最大小时跨度；
	    ui_max_dtn_inter_hr_log_max_dtn_inter_hr = ui_lately_inter_dtn_hr*1.0 / Math.log(ui_max_dtn_inter_hr); // 15、u对i最后行为距离预测天小时数 * log (u对i行为的最大小时跨度);
    }
}


