package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.constant.KeyConstant;
import oracle.demo.oow.bd.pojo.ActivityType;
import oracle.demo.oow.bd.pojo.BooleanType;
import oracle.demo.oow.bd.pojo.RatingType;

import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.MovieTO;

import oracle.demo.oow.bd.util.FileWriterUtil;
import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;
import oracle.kv.table.IndexKey;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.apache.avro.Schema;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter;

import org.codehaus.jackson.node.ObjectNode;

import com.sleepycat.je.utilint.DbLsn;

public class ActivityDAO {

    public ActivityDAO() {
       
    }

    public void insertCustomerActivity(ActivityTO activityTO) throws Exception {
        int custId = 0;
        int movieId = 0;
        ActivityType activityType = null;
        String jsonTxt = null;

        if (activityTO != null) {
            jsonTxt = activityTO.getJsonTxt();
            FileWriterUtil.writeOnFile(activityTO.getActivityJsonOriginal().toString());
            
            custId = activityTO.getCustId();
            movieId = activityTO.getMovieId();

            if (custId > 0 && movieId > 0) {
            	  activityType = activityTO.getActivity();

                  HBaseDB db = HBaseDB.getInstance();
                  
                  Long id = db.getId(ConstantsHBase.TABLE_GID,ConstantsHBase.FAMILY_GID_GID,ConstantsHBase.QUALIFIER_GID_ACTIVITY_ID);
                  
                  Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
                  
                  List<Put> puts = new ArrayList<Put>();
                  Put put = new Put(Bytes.toBytes(id));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),Bytes.toBytes(activityTO.getActivity().getValue()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),Bytes.toBytes(activityTO.getMovieId()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID),Bytes.toBytes(activityTO.getGenreId()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION),Bytes.toBytes(activityTO.getPosition()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE),Bytes.toBytes(activityTO.getPrice()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING),Bytes.toBytes(activityTO.getRating().getValue()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),Bytes.toBytes(activityTO.getCustId()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED),Bytes.toBytes(activityTO.isRecommended().getValue()));
                  put.addColumn(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME),Bytes.toBytes(activityTO.getTimeStamp()));
          
                 try
				{
					table.put(put);
				} catch (IOException e)
				{
					e.printStackTrace();
				}          
            } //if (custId > 0 && movieId > 0)

        } //if (activityTO != null)

    } //insetCustomerActivity
    
    public ActivityTO getActivityTO(int custId, int movieId) throws Exception {
    	HBaseDB db = HBaseDB.getInstance();
    	Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
    	Scan scan = new Scan();
    	Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(custId));
    	((SingleColumnValueFilter)filter).setFilterIfMissing(true);
    	Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(movieId));
    	((SingleColumnValueFilter)filter1).setFilterIfMissing(true);
    	FilterList filterList = new FilterList(filter,filter1);
		scan.setFilter(filterList);
		ResultScanner resultScanner = table.getScanner(scan);
		ActivityTO activityTO = new ActivityTO();
		if(resultScanner != null){
			Iterator<Result> iter = resultScanner.iterator();
			while(iter.hasNext()){
				Result result = iter.next();
				if(result!=null&!result.isEmpty()){
					activityTO.setCustId(custId);
					activityTO.setCustId(movieId);
					activityTO.setGenreId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_GENRE_ID))));
					int activity = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY)));					
					activityTO.setTimeStamp(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_TIME))));
					activityTO.setPrice(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_PRICE))));
					activityTO.setPosition(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_POSITION))));
					String recommended=Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RECOMMENDED)));
					int rating=Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_RATING)));
					activityTO.setCustId(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY),Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID))));
					RatingType ratingType = null;
					activityTO.setRating(ratingType.getType(rating));
					BooleanType booleanType = null;
					activityTO.setRecommended(booleanType.getType(recommended));
					ActivityType activityType = null;
					activityTO.setActivity(activityType.getType(activity));
				}
			}
        
		}  
        return activityTO;
    } //getActivityTO

	
	public int getIdByUserIdAndMovieId(int userId, int movieId) {
		int id=-1;
    	HBaseDB db=HBaseDB.getInstance();
    	Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
    	Scan scan = new Scan();
    	// 设置过滤器
    	FilterList filterList=new FilterList();   	
    	Filter filter = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
    			Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
    			CompareFilter.CompareOp.EQUAL,Bytes.toBytes(userId));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);
    	Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), 
    			Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID),
    			CompareFilter.CompareOp.EQUAL,Bytes.toBytes(movieId));
    	((SingleColumnValueFilter) filter2).setFilterIfMissing(true);
    	filterList.addFilter(filter);
    	filterList.addFilter(filter2);
    	scan.setFilter(filterList);
    	try
		{
    		ResultScanner resultScanner =table.getScanner(scan);
			if(resultScanner!=null)
			{
				Iterator<Result> iterator=resultScanner.iterator();
				while(iterator.hasNext())
				{
					Result result=iterator.next();
					if(result!=null)
					{
						id=Bytes.toInt(result.getRow());
						System.out.println(id);
					}
				}
			}
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return id;
	}

	public List<Integer> getActivityIdsByUserId(int userId) throws Exception {
		HBaseDB db=HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
    	ArrayList<Integer> activityIds =new ArrayList<Integer>();
    	Scan scan=new Scan();
    	Filter filter = new SingleColumnValueFilter(
				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
				CompareFilter.CompareOp.EQUAL, Bytes.toBytes(userId));
    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);  	
    	scan.setFilter(filter);
		ResultScanner results = table.getScanner(scan); 	
    	for (Result result : results)
		{
    		activityIds.add(Bytes.toInt(result.getRow()));
    	    
		}
    	return activityIds;
	}
	
	 public List<MovieTO> getCustomerBrowseList(int custId) throws Exception{
				int activity = 5;
				MovieDAO movieDAO = new MovieDAO();
				List<MovieTO> movieList = movieDAO.getMoviesCurrentWatchList(custId, activity);
				return movieList;
			}
	 public List<MovieTO> getCustomerCurrentWatchList(int custId) throws Exception {
		    int activity = 5;
			MovieDAO movieDAO = new MovieDAO();
			List<MovieTO> movieList = movieDAO.getMoviesCurrentWatchList(custId, activity);
	        return movieList;
	    }
	 public List<MovieTO> getCommonPlayList() throws Exception {
		    int activity = 5;
			MovieDAO movieDAO = new MovieDAO();
			List<MovieTO> movieList = movieDAO.getCommonPlayList(activity);
			return movieList;
	    }
	 public List<MovieTO> getCustomerHistoricWatchList(int custId) throws Exception{
			int activity = 5;
			MovieDAO movieDAO = new MovieDAO();
			List<MovieTO> movieList = movieDAO.getMoviesCurrentWatchList(custId, activity);
			return movieList;
		}
}//ActivityDAO
