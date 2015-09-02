package lm.group.mapreduce;

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
	// 5*4= 20 ,d1~d20 ; +day1_2
	private int[][] i_day_dtx_tpy_amount ;//i_day1_14_view_amount;  //被tpy总数
    // 4*4= 16 个  , d21~d36 ; +day1_2
    private double[][] i_day_dtx_avg_tpy_days_count; //平均每天被tpy次数
    //  5*4=20 , d37~d56 ; +day1_2
    private int[][] i_day_dtx_tpy_users ; //tpy的用户数
    private Set[][] i_day_dtx_tpy_usersSet;
    //  5*4= 20 个 ,  d57~d76 ; +day1_2
    private double i_day_dtx_avg_tpy_times_per_user_count[][]; //每个用户平均tpy次数

    // ###########  复合特征  #######################
    //  5*3=15 ， 5个时间划分，3种行为,d77~d91,  ; +day1_2
    //  no i_day_dtx_view_ratio_a  
    private double[][] i_day_dtx_tpy_ratio_a; // 被tpx数/被行为总次数
    //  5*3=15 ,d92~d106  ; +day1_2
    private double[][] i_day_dtx_tpy_ratio_u ; //被收藏用户数/总交互用户数 
    // 5*4 = 20 , d107~d126 ; +day1_2 
    private double[] i_day_dtx_cart2view_ratio; // cart数/view数
    private double[] i_day_dtx_cart2buy_ratio; // cart数/buy数
    private double[] i_day_dtx_collect2view_ratio; // collect数/view数
    private double[] i_day_dtx_collect2buy_ratio; // collect数/buy数
    // 5*4 = 20 , d127~d146  ; +day1_2 
    private double[] i_day_dtx_cart2view_ratio_log_cart; // cart数/view数 * log(cart)
    private double[] i_day_dtx_cart2buy_ratio_log_cart; // cart数/buy数 * log(cart)
    private double[] i_day_dtx_collect2view_ratio_log_collect; // collect数/view数* log(collect)
    private double[] i_day_dtx_collect2buy_ratio_log_collect; // collect数/buy数* log(collect)
    // 4*4=16 ,d147~d162     ; +day1_2 
    private double i_day1_7_to_1_14_view_amount_hot;  //1-7天被点击总数/1-14天点击总数
    private double i_day1_3_to_1_7_view_amount_hot;  //1-3天被点击总数/1-7天点击总数
    private double i_day1_2_to_1_3_view_amount_hot;  //1天被点击总数/1-3天点击总数
    private double i_day1_to_1_2_view_amount_hot;  //1天被点击总数/1-2天点击总数
    
    private double i_day1_7_to_1_14_collect_amount_hot;
    private double i_day1_3_to_1_7_collect_amount_hot;
    private double i_day1_2_to_1_3_collect_amount_hot;
    private double i_day1_to_1_2_collect_amount_hot;
    
    private double i_day1_7_to_1_14_cart_amount_hot;
    private double i_day1_3_to_1_7_cart_amount_hot;
    private double i_day1_2_to_1_3_cart_amount_hot;
    private double i_day1_to_1_2_cart_amount_hot;
    
    private double i_day1_7_to_1_14_buy_amount_hot;
    private double i_day1_3_to_1_7_buy_amount_hot;
    private double i_day1_2_to_1_3_buy_amount_hot;
    private double i_day1_to_1_2_buy_amount_hot;
    // d163~d166
    private int i_day1_7_to_1_14_buy_amount_fresh; //
    private int i_day1_7_to_1_14_collect_amount_fresh;  //否
    private int i_day1_7_to_1_14_view_amount_fresh;  //否
    private int i_day1_7_to_1_14_cart_amount_fresh;  //否
    // d167 ~ d169 
    private int i_day1_14_buy_2_times_uv;  // 购买了两次 或两次以上的 用户数
    private Set i_day1_14_buy_2_times_uvSet ;
    private int i_day1_7_buy_2_times_uv;  
    private Set i_day1_7_buy_2_times_uvSet ;
    private double i_day1_7_to_1_14_buy_2_times_uv_fresh;  // 回头客变化趋势 （i_day1_7_buy_2_times_uv)/( i_day1_14_buy_2_times_uv )

    
    
    // hour level  特征 
    
    
    
    private int[] ruler ;
    private double MC0 ; 

    public void setup(TaskContext context) throws IOException {
        result = context.createOutputRecord();
    }

    public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
        
    	init();
    	
    	SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
 		Date predDay = new Date();
		try {
			predDay = df.parse("2014-12-19");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        while (values.hasNext()) {
            Record val = values.next();
            
            String uid = val.getString("uid");
            String ic = val.getString("ic");
            int tp = new Long(val.getBigint("tp")).intValue();
            String ug = val.getString("ug");
            int hr = new Long(val.getBigint("hr")).intValue();
            String dt = val.getString("dt");
//            String[] parts = dt.split("-");
//            int dt_m = Integer.parseInt(parts[0]);
//            int dt_d = Integer.parseInt(parts[1]);             
            
    		Date date = new Date();
			try {
				date = df.parse(dt);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		long I = predDay.getTime() - date.getTime() ;
    		long timeDistance = I/(24*60*60*1000);
    		
    		int dtLevel =  dateCrossLevel(timeDistance);
    		if (dtLevel ==0){continue;};
    		
    		makeSingleFeatures(  uid , ic ,  tp, ug ,  hr ,  dtLevel);
        }
        
        getFeatures();
        getFeatures2();
        
        result.set(0,key.get("iid"));
        
        int counter =1;
        ArrayList fList = new ArrayList(169);
        
        for (int i =0 ; i<5; i++){
        	for (int j =0; j <4 ; j++){
        		fList.add(i_day_dtx_tpy_amount[i][j]) ;
        	}
        }
        for (int i =0 ; i<4; i++){
        	for (int j =0; j <4 ; j++){
        		fList.add(i_day_dtx_avg_tpy_days_count[i][j]) ;
        	}
        }
        
        for (int i =0 ; i<5; i++){
        	for (int j =0; j <4 ; j++){
        		fList.add(i_day_dtx_tpy_users[i][j]) ;
        	}
        }
        for (int i =0 ; i<5; i++){
        	for (int j =0; j <4 ; j++){
        		fList.add(i_day_dtx_avg_tpy_times_per_user_count[i][j]) ;
        	}
        }
        for (int i =0 ; i<5; i++){
        	for (int j =0; j <3 ; j++){
        		fList.add(i_day_dtx_tpy_ratio_a[i][j]) ;
        	}
        }
        for (int i =0 ; i<5; i++){
        	for (int j =0; j <3 ; j++){
        		fList.add(i_day_dtx_tpy_ratio_u[i][j]) ;
        	}
        }
        for (int i=0 ; i<5;i++){
           //cart数/buy数
        	fList.add( i_day_dtx_cart2view_ratio[i] ) ;
        	//cart数/buy数
        	fList.add( i_day_dtx_cart2buy_ratio[i] ) ;
     	    //collect数/view数
        	fList.add( i_day_dtx_collect2view_ratio[i] ) ;
     	    //collect数/buy数
        	fList.add( i_day_dtx_collect2buy_ratio[i] ) ;
        	
        }
        for (int i=0 ; i<5;i++){
          //cart数/buy数 *log( cart)
        	fList.add( i_day_dtx_cart2view_ratio_log_cart[i] ) ;
        	//cart数/buy数 *log( cart)
        	fList.add( i_day_dtx_cart2buy_ratio_log_cart[i] ) ;
     	    //collect数/view数
        	fList.add( i_day_dtx_collect2view_ratio_log_collect[i] ) ;
     	    //collect数/buy数 *log( cart)
        	fList.add( i_day_dtx_collect2buy_ratio_log_collect[i] ) ;
        	
        }
//        // 特征向量是 0向量的不输出（还是，在最后连接的时候过滤掉0特征向量？）
        // 4*4=16 ,d147~d162     ; +day1_2 
        fList.add(i_day1_7_to_1_14_view_amount_hot) ;
        fList.add(i_day1_3_to_1_7_view_amount_hot) ;
        fList.add(i_day1_2_to_1_3_view_amount_hot) ;
        fList.add(i_day1_to_1_2_view_amount_hot) ;
        fList.add(i_day1_7_to_1_14_collect_amount_hot) ;
        fList.add(i_day1_3_to_1_7_collect_amount_hot) ;
        fList.add(i_day1_2_to_1_3_collect_amount_hot) ;
        fList.add(i_day1_to_1_2_collect_amount_hot) ;
        fList.add(i_day1_7_to_1_14_cart_amount_hot) ;
        fList.add(i_day1_3_to_1_7_cart_amount_hot) ;
        fList.add(i_day1_2_to_1_3_cart_amount_hot) ;
        fList.add(i_day1_to_1_2_cart_amount_hot) ;
        fList.add(i_day1_7_to_1_14_buy_amount_hot) ;
        fList.add(i_day1_3_to_1_7_buy_amount_hot) ;
        fList.add(i_day1_2_to_1_3_buy_amount_hot) ;
        fList.add(i_day1_to_1_2_buy_amount_hot) ;
       // d163~d166
        fList.add(i_day1_7_to_1_14_buy_amount_fresh ) ; 
        fList.add(i_day1_7_to_1_14_collect_amount_fresh ) ; 
        fList.add(i_day1_7_to_1_14_view_amount_fresh ) ; 
        fList.add(i_day1_7_to_1_14_cart_amount_fresh ) ; 
        // d167 ~ d169 
        fList.add(i_day1_14_buy_2_times_uv ) ; 
        fList.add(i_day1_7_buy_2_times_uv ) ; 
        fList.add(i_day1_7_to_1_14_buy_2_times_uv_fresh ) ; 
        
//        double fsumOfF =0.0 ;
//        // check 0 vecotr 
//        for(Iterator it = fList.iterator();it.hasNext();){
//        	fsumOfF += Double.valueOf(it.next().toString() ) ;
//       }
//        if ( fsumOfF < MC0)return ;
        
        // output 
        for(Iterator it = fList.iterator();it.hasNext();){
            result.set( counter ,it.next());
            counter ++;
       }
        
//        for (int i =0 ; i<5 ; i++)
//	        for (int j =0 ; j <3; j ++){
//	        	result.set( counter ,  i_day_dtx_tpy_ratio_u[i][j]);
//	        	counter ++ ; 
//	        }
//        
// 
//        for (int i=0 ; i <5 ; i++ ){
//        	result.set( counter ,  i_day_dtx_cart2view_ratio_log_cart[i]);counter ++ ;
//        	result.set( counter ,  i_day_dtx_cart2buy_ratio_log_cart[i]);counter ++ ;
//        	result.set( counter ,  i_day_dtx_collect2view_ratio_log_collect[i]);counter ++ ;
//        	result.set( counter ,  i_day_dtx_collect2buy_ratio_log_collect[i]);counter ++ ;
//        
//        }
        
        
        context.write(result);
    }

    public void cleanup(TaskContext arg0) throws IOException {

    }
    
    public void makeSingleFeatures( String uid ,String ic , int tp,String ug , int hr , int dtLevel){
    	// making i_day_dtx_tpy_amount features ; 被 tpy 总数
    	// d1~d16 
    	for(int i =0 ; i< dtLevel;i++){
    		// d1~d20
    		i_day_dtx_tpy_amount[4-i][tp-1] +=1;  		
    	}
    	
    	// making i_day_dtx_tpy_usersSet //tpy的用户数Set 收集
    	for(int i =0 ; i< dtLevel;i++){
    		if ( tp == 4 ){
	    		if( i == 1  ){ // day1_7
	    			if ( i_day_dtx_tpy_usersSet[4-i][tp-1].contains(uid) ){
	    				i_day1_7_buy_2_times_uvSet.add(uid) ;
	    			}
	    		}
	    		if( i ==0){// day1_7 
	    			if ( i_day_dtx_tpy_usersSet[4-i][tp-1].contains(uid) ){
	    				i_day1_14_buy_2_times_uvSet.add(uid);
	    			}    			
	    		}
    		}
    		i_day_dtx_tpy_usersSet[4-i][tp-1].add(uid);
    	}
    	
    	// d129~d132
        i_day1_7_to_1_14_buy_amount_fresh = (int) Math.round(i_day1_7_to_1_14_buy_amount_hot + 0.0001); 
        i_day1_7_to_1_14_collect_amount_fresh = (int) Math.round(i_day1_7_to_1_14_collect_amount_hot + 0.0001 );  
        i_day1_7_to_1_14_view_amount_fresh = (int) Math.round(i_day1_7_to_1_14_view_amount_hot  + 0.0001 );  
        i_day1_7_to_1_14_cart_amount_fresh = (int) Math.round(i_day1_7_to_1_14_cart_amount_hot  + 0.0001 );  
    	
    	//  d133 ~ d135
    	i_day1_14_buy_2_times_uv = i_day1_14_buy_2_times_uvSet.size();
    	i_day1_7_buy_2_times_uv = i_day1_7_buy_2_times_uvSet.size();
    	// 回头客变化趋势
    	i_day1_7_to_1_14_buy_2_times_uv_fresh = i_day1_7_buy_2_times_uv * 1.0 / (i_day1_14_buy_2_times_uv + MC0 );
    	
	}
    public void getFeatures(){
    	// get i_day_dtx_avg_tpy_days_count features ;//平均每天被XX次数
    	// d17~d28
    	for(int i =0 ; i<4;i++){
    		for(int j=0 ; j<4 ; j++){
    			
    			i_day_dtx_avg_tpy_days_count[i][j] = i_day_dtx_tpy_amount[i+1][j] * 1.0 / ruler[i+1];
    		}
    	}
    	
    	// get i_day_dtx_tpy_users //tpy的用户数
    	// d29~d44
    	for(int i =0 ; i<5;i++){
    		for(int j=0 ; j<4 ; j++){
    			i_day_dtx_tpy_users[i][j] = i_day_dtx_tpy_usersSet[i][j].size();
    		}
    	}
    	
    	// 4*4=16 ,
    	// d45~d60
       //每个用户平均tpy次数
        for(int i=0;i<5;i++ ){
        	for(int j=0;j<4;j++){
        		i_day_dtx_avg_tpy_times_per_user_count[i][j] = i_day_dtx_tpy_amount[i][j] * 1.0 / (i_day_dtx_tpy_users[i][j] + MC0 ) ;
        	}
        }	
    }
    
    public void getFeatures2 () {
    	
    	// d61~d72 
    	int [] sumTpy = new int[5] ;
    	for( int i=0; i<5;i++){
    		for(int j=0;j<4;j++){
    			sumTpy[i] += i_day_dtx_tpy_amount[i][j] ;
    		}
    	}
    	for( int i=0; i<5;i++){
    		for(int j=1;j<4;j++){
    			i_day_dtx_tpy_ratio_a[i][j-1] = i_day_dtx_tpy_amount[i][j]*1.0 / (sumTpy[i] + MC0 )  ;
    		}
    	}
    	
        //  4*3=12 , d73~d84
	   // i_day_dtx_tpy_ratio_u = new double[5][3] ; //被tpy用户数/总交互用户数
	    Set[] sumTpySet = new HashSet[5];
	    for( int i=0; i<5;i++){
	    	sumTpySet[i] = new HashSet() ; 
    		for(int j=0;j<4;j++){
    			sumTpySet[i].addAll( i_day_dtx_tpy_usersSet[i][j] ) ;
    		}
    	}
	    for( int i=0; i<5;i++){
    		for(int j=1;j<4;j++){
    			i_day_dtx_tpy_ratio_u[i][j-1] = i_day_dtx_tpy_users[i][j]*1.0 / (sumTpySet[i].size() + MC0 )  ;
    		}
    	}
	    
	    // 4*4 = 16 , d85~d100 , d101~d116
	    for (int i =0; i<5;i++){
	    	//cart数/view数
	    	i_day_dtx_cart2view_ratio[i] = i_day_dtx_tpy_amount[i][2]*1.0 / ( i_day_dtx_tpy_amount[i][0] + MC0 ) ;
	    	//cart数/view数 * log(cart)
	    	i_day_dtx_cart2view_ratio_log_cart[i] = i_day_dtx_cart2view_ratio[i] * Math.log(i_day_dtx_tpy_amount[i][2]*1.0 + MC0 );
	    	//cart数/buy数
	 	    i_day_dtx_cart2buy_ratio[i] = i_day_dtx_tpy_amount[i][2]*1.0 / ( i_day_dtx_tpy_amount[i][3] + MC0 ) ;
	 	    //cart数/buy数 * log(cart)
	 	    i_day_dtx_cart2buy_ratio_log_cart[i] = i_day_dtx_cart2buy_ratio[i] * Math.log(i_day_dtx_tpy_amount[i][2]*1.0 + MC0 );
	 	    //collect数/view数
	 	    i_day_dtx_collect2view_ratio[i] = i_day_dtx_tpy_amount[i][1]*1.0 / ( i_day_dtx_tpy_amount[i][0] + MC0 ) ;
	 	    //collect数/view数* log(collect)
	 	    i_day_dtx_collect2view_ratio_log_collect[i] = i_day_dtx_collect2view_ratio[i] * Math.log(  i_day_dtx_tpy_amount[i][1]*1.0  + MC0  );
	 	    //collect数/buy数
	 	    i_day_dtx_collect2buy_ratio[i] = i_day_dtx_tpy_amount[i][1]*1.0 / ( i_day_dtx_tpy_amount[i][3] + MC0 ) ;
	 	    //collect数/buy数* log(collect)
	 	    i_day_dtx_collect2buy_ratio_log_collect[i] = i_day_dtx_collect2buy_ratio[i] * Math.log(  i_day_dtx_tpy_amount[i][1]*1.0  + MC0  );
	    }
	    
	    // 3*4=12 , d117~d128
    	i_day1_7_to_1_14_view_amount_hot = i_day_dtx_tpy_amount[3][0] * 1.0 / (i_day_dtx_tpy_amount[4][0] + MC0);  //1-7天被点击总数/1-14天点击总数
    	i_day1_3_to_1_7_view_amount_hot = i_day_dtx_tpy_amount[2][0] * 1.0 / (i_day_dtx_tpy_amount[3][0] + MC0);  //1-3天被点击总数/1-7天点击总数
    	i_day1_2_to_1_3_view_amount_hot = i_day_dtx_tpy_amount[1][0] * 1.0 / (i_day_dtx_tpy_amount[2][0] + MC0);  //1天被点击总数/1-3天点击总数
    	i_day1_to_1_2_view_amount_hot = i_day_dtx_tpy_amount[0][0] * 1.0 / (i_day_dtx_tpy_amount[1][0] + MC0);  //1天被点击总数/1-3天点击总数
    	
    	i_day1_7_to_1_14_collect_amount_hot = i_day_dtx_tpy_amount[3][1] * 1.0 / (i_day_dtx_tpy_amount[4][1] + MC0) ;
    	i_day1_3_to_1_7_collect_amount_hot = i_day_dtx_tpy_amount[2][1] * 1.0 / (i_day_dtx_tpy_amount[3][1] + MC0) ;
    	i_day1_2_to_1_3_collect_amount_hot = i_day_dtx_tpy_amount[1][1] * 1.0 / (i_day_dtx_tpy_amount[2][1] + MC0) ;
    	i_day1_to_1_2_collect_amount_hot = i_day_dtx_tpy_amount[0][1] * 1.0 / (i_day_dtx_tpy_amount[1][1] + MC0) ;
    	
    	i_day1_7_to_1_14_cart_amount_hot = i_day_dtx_tpy_amount[3][2] * 1.0 / (i_day_dtx_tpy_amount[4][2] + MC0)  ;
    	i_day1_3_to_1_7_cart_amount_hot = i_day_dtx_tpy_amount[2][2] * 1.0 / (i_day_dtx_tpy_amount[3][2] + MC0);
    	i_day1_2_to_1_3_cart_amount_hot = i_day_dtx_tpy_amount[1][2] * 1.0 / (i_day_dtx_tpy_amount[2][2] + MC0);
    	i_day1_to_1_2_cart_amount_hot = i_day_dtx_tpy_amount[0][2] * 1.0 / (i_day_dtx_tpy_amount[1][2] + MC0);
    	
    	i_day1_7_to_1_14_buy_amount_hot = i_day_dtx_tpy_amount[3][3] * 1.0 / (i_day_dtx_tpy_amount[4][3] + MC0);
    	i_day1_3_to_1_7_buy_amount_hot = i_day_dtx_tpy_amount[2][3] * 1.0 / (i_day_dtx_tpy_amount[3][3] + MC0);
    	i_day1_2_to_1_3_buy_amount_hot = i_day_dtx_tpy_amount[1][3] * 1.0 / (i_day_dtx_tpy_amount[2][3] + MC0);
    	i_day1_to_1_2_buy_amount_hot = i_day_dtx_tpy_amount[0][3] * 1.0 / (i_day_dtx_tpy_amount[1][3] + MC0);
    	
    	
    	
    	// d129~d132
        i_day1_7_to_1_14_buy_amount_fresh = (int) Math.round(i_day1_7_to_1_14_buy_amount_hot + 0.0001); 
        i_day1_7_to_1_14_collect_amount_fresh = (int) Math.round(i_day1_7_to_1_14_collect_amount_hot + 0.0001 );  
        i_day1_7_to_1_14_view_amount_fresh = (int) Math.round(i_day1_7_to_1_14_view_amount_hot  + 0.0001 );  
        i_day1_7_to_1_14_cart_amount_fresh = (int) Math.round(i_day1_7_to_1_14_cart_amount_hot  + 0.0001 );  
    }
    
    public int dateCrossLevel( long d){
    	if (d ==1 )return 5;
    	else if (d <=2 ) return 4 ;
    	else if (d <=3 ) return 3;
    	else if (d <= 7) return 2 ;
    	else if (d <= 14) return 1 ;
    	return 0;
    }
    
    public void init(){
    	i_day1_14_buy_2_times_uv = 0;
    	i_day1_14_buy_2_times_uvSet = new  HashSet() ;
    	i_day1_7_buy_2_times_uv = 0;  //否
    	i_day1_7_buy_2_times_uvSet = new  HashSet() ;
    	i_day1_7_to_1_14_buy_2_times_uv_fresh = 0;  //（i_day1_7_buy_2_times_uv)/( i_day1_14_buy_2_times_uv )

    	//  3*4=12个
    	i_day_dtx_avg_tpy_days_count = new double[4][4] ; //平均每天被XX次数
    	
    	 //  4*4=16个
        i_day_dtx_avg_tpy_times_per_user_count = new double[5][4]; //每个用户平均tpy次数

    	// 4*4=16
    	i_day_dtx_tpy_amount = new int[5][4] ;

    	//  4*4=16
    	i_day_dtx_tpy_users = new int[5][4] ;//tpy的用户数
        i_day_dtx_tpy_usersSet = new HashSet[5][4];
		for (int i=0; i<5;i++){
			for (int j =0 ;j <4; j++){
				i_day_dtx_tpy_usersSet[i][j] = new HashSet();
			}
		}
    
    	// ###########  复合特征  ######################
		//  4*3=12  ,4个时间划分，3种行为,d61~d72,
	    //  no i_day_dtx_view_ratio_a  
	    i_day_dtx_tpy_ratio_a = new double[5][3]; // 被tpy数/被行为总次数

    	//  4*3=12 , d73~d84
	    i_day_dtx_tpy_ratio_u = new double[5][3] ; //被tpy用户数/总交互用户数
	
	    // 4*4 = 16 , d85~d100
	    i_day_dtx_cart2view_ratio = new double[5] ;//cart数/view数
	    i_day_dtx_cart2buy_ratio = new double[5] ;//cart数/buy数
	    i_day_dtx_collect2view_ratio = new double[5] ;//collect数/view数
	    i_day_dtx_collect2buy_ratio = new double[5] ; //collect数/buy数
	    
	    // 4*4 = 16 , d101~d116
	    i_day_dtx_cart2view_ratio_log_cart = new double[5] ;//cart数/view数 * log(cart)
	    i_day_dtx_cart2buy_ratio_log_cart = new double[5] ;//cart数/buy数 * log(cart)
	    i_day_dtx_collect2view_ratio_log_collect = new double[5] ;//collect数/view数* log(collect)
	    i_day_dtx_collect2buy_ratio_log_collect = new double[5]; //collect数/buy数* log(collect)

    	// 3*4=12 , d117~d128
    	i_day1_7_to_1_14_view_amount_hot = 0;  //1-7天被点击总数/1-14天点击总数
    	i_day1_3_to_1_7_view_amount_hot = 0;  //1-3天被点击总数/1-7天点击总数
    	i_day1_2_to_1_3_view_amount_hot = 0 ;  
    	i_day1_to_1_2_view_amount_hot = 0;  //1天被点击总数/1-3天点击总数
    	i_day1_7_to_1_14_collect_amount_hot = 0;
    	i_day1_3_to_1_7_collect_amount_hot = 0;
    	i_day1_2_to_1_3_collect_amount_hot = 0 ;
    	i_day1_to_1_2_collect_amount_hot = 0;
    	i_day1_7_to_1_14_cart_amount_hot = 0;
    	i_day1_3_to_1_7_cart_amount_hot = 0;
    	i_day1_2_to_1_3_cart_amount_hot = 0 ;
    	i_day1_to_1_2_cart_amount_hot = 0;
    	i_day1_7_to_1_14_buy_amount_hot = 0;
    	i_day1_3_to_1_7_buy_amount_hot = 0;
    	i_day1_2_to_1_3_buy_amount_hot = 0 ;
    	i_day1_to_1_2_buy_amount_hot = 0;
    	// 
    	i_day1_7_to_1_14_buy_amount_fresh = 0;
    	i_day1_7_to_1_14_collect_amount_fresh = 0;  //否
    	i_day1_7_to_1_14_view_amount_fresh = 0;  //否
    	i_day1_7_to_1_14_cart_amount_fresh = 0;  //否
    	
    	ruler = new int[] {1,2,3,7,14}  ;
    	MC0 = 0.0000001;
    }
    
    
    public  double sigmoid( double x ){
		return 1.0/(1.0+Math.exp(-1.0*x)) ;
		
	}
    
    
}
