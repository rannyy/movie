package oracle.demo.oow.bd.dao.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.ActivityTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;

public class CustomerDao {

	public int  MOVIE_MAX_COUNT = 25;


	//修改user中的info和id列族，导入表
	public void insert(CustomerTO customerTO) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("CUSTOMER");
		if(table!=null) {
			Put put = new Put(Bytes.toBytes(customerTO.getUserName()));//行健
			//username --> id的映射
			put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(customerTO.getId()));
			//用户的基本信息
			Put put2 = new Put(Bytes.toBytes(customerTO.getId()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(customerTO.getName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("email"), Bytes.toBytes(customerTO.getEmail()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("username"), Bytes.toBytes(customerTO.getUserName()));
			put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("password"), Bytes.toBytes(customerTO.getPassword()));
			
			List<Put> puts = new ArrayList();
			puts.add(put);
			puts.add(put2);
			try {
				table.put(puts);
				table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	//判断登陆
	public CustomerTO getCustomerByCredential(String username, String password) {
		CustomerTO customerTO = null;
		
		//首先通过username查询id
		int id = getIdByUserName(username);
		if(id>0) {
			customerTO = getInfoById(id);
			if(customerTO!=null)
			{
				if(!customerTO.getPassword().equals(password))
				{
					 customerTO=null;
				}
			}
		}
		//根据id查询基本信息
		return customerTO;
	}

	//通过id获取所有的信息保存到customerTo中
	private CustomerTO getInfoById(int id) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("CUSTOMER");
		Get get = new Get(Bytes.toBytes(id));
		CustomerTO customerTO = new CustomerTO();
		try {
			Result result = table.get(get);
			customerTO.setEmail(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("email"))));
			customerTO.setName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
			customerTO.setId(id);
			customerTO.setPassword(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("password"))));
			customerTO.setUserName(Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return customerTO;
	}
 
	//通过username查询id
	public int getIdByUserName(String username) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable("CUSTOMER");
		Get get = new Get(Bytes.toBytes(username));//获取行健
		int id = 0;
		try {
			Result result = table.get(get);
			id = Bytes.toInt(result.getValue(Bytes.toBytes("id"), Bytes.toBytes("id")));	
		} catch (IOException e) {
			e.printStackTrace();
		}
		return id;
	}
	
	
	//两个过滤器
		/*
		 * 
		 * 
		 * 根据分类的id获取电影的信息
		 */
		public List<MovieTO> getMovies4CustomerByGenre(int custId,int genreId){
			HBaseDB hBaseDB = HBaseDB.getInstance();
			Table table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
			Scan scan = new Scan();
			scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE));
			Filter filter = new PrefixFilter(Bytes.toBytes(genreId+"_"));
			Filter filter2 = new PageFilter(MOVIE_MAX_COUNT);
			FilterList filterList = new FilterList(filter,filter2);
			scan.setFilter(filterList);
			
			
			ResultScanner resultScanner = null;
			try {
				resultScanner = table.getScanner(scan);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			List<MovieTO> movieTos  =new ArrayList();
			MovieTO movieTO = null;
			if(resultScanner != null){
				Iterator<Result> iter = resultScanner.iterator();
				MovieDAO movieDAO = new MovieDAO();
				while(iter.hasNext()){
					Result result = iter.next();
					if(result!=null&!result.isEmpty()){
						int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID)));
					    movieTO = movieDAO.getMovieById(movieId);
						if(StringUtil.isNotEmpty(movieTO.getPosterPath())){
							movieTO.setOrder(100);
						}else{
							movieTO.setOrder(0);
						}
						movieTos.add(movieTO);
					}
				}			
				
			}
			Collections.sort(movieTos);
			return movieTos;			
		}
		
		 /**
	     * This method returns ActivitTO that has customer's movie rating
	     * @param custId
	     * @param movieId
	     * @return ActivityTO
		 * @throws Exception 
	     */	
		 public ActivityTO getMovieRating(int custId, int movieId) throws Exception {
			 ActivityDAO activityDAO=new ActivityDAO();
			 ActivityTO activityTO;
			 activityTO=activityDAO.getActivityTO(custId, movieId);
			 return activityTO;
		 }
}
