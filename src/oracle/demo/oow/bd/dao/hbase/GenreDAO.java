package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.GenreMovieTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;

public class GenreDAO{
	
	
	public void insertGenre(GenreTO genreTo) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE, genreTo.getId(), ConstantsHBase.FAMILY_GENRE_GENRE, ConstantsHBase.QUALIFIER_GENRE_NAME, genreTo.getName());
	}
	
	public void insertGenreMovie(MovieTO movieTo,GenreTO genreTo) {
		HBaseDB db = HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_GENRE, genreTo.getId()+"_"+movieTo.getId(), ConstantsHBase.FAMILY_GENRE_MOVIE, ConstantsHBase.QUALIFIER_GENRE_MOVIE_ID, movieTo.getId());
	}

	//两个过滤器
	/*
	 * 
	 * 
	 * 根据分类的id获取电影的信息
	 */
	public List<GenreMovieTO> getMovies4Customer(int custId,int movieMaxCount,int genreMaxCount){
		List<GenreMovieTO> genreTOs = new ArrayList();
		Scan scan = new Scan();
		
		Filter filter = new PageFilter(genreMaxCount);
		scan.setFilter(filter);
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE));
		HBaseDB db = HBaseDB.getInstance();
		Table table = db.getTable(ConstantsHBase.TABLE_GENRE);
		
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
			Iterator<Result> iter = resultScanner.iterator();
			GenreTO genreTO = null;
			while(iter.hasNext()){
				genreTO = new GenreTO();
				Result result = iter.next();
				genreTO.setId(Bytes.toInt(result.getRow()));
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
				GenreMovieTO genreMovieTO = new GenreMovieTO();
				genreMovieTO.setGenreTO(genreTO);
				genreTOs.add(genreMovieTO);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return genreTOs;		
	}

	public GenreTO getGenreById(int genreId) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_GENRE);
		Get get = new Get(Bytes.toBytes(genreId));
		GenreTO genreTO = new GenreTO();
		try {
			Result result = table.get(get);
			if(!result.isEmpty()){
				genreTO.setId(genreId);
				genreTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_GENRE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_GENRE_NAME))));
			}
			} catch (IOException e) {
			e.printStackTrace();
		}
		return genreTO;
	}
 
}
