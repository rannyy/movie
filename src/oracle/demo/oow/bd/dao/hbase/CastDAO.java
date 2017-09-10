package oracle.demo.oow.bd.dao.hbase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import oracle.demo.oow.bd.to.CastMovieTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;


import oracle.demo.oow.bd.util.KeyUtil;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class CastDAO{
   
    public void insertCastInfo(CastTO castTO) {     
        HBaseDB hBaseDB = HBaseDB.getInstance();
		hBaseDB.put(ConstantsHBase.TABLE_CAST, castTO.getId(), ConstantsHBase.FAMILY_CAST_CAST, ConstantsHBase.QUALIFIER_CAST_NAME, castTO.getName());
    
		insertCastToMovie(castTO);
    }

	private void insertCastToMovie(CastTO castTO) {
		HBaseDB db=HBaseDB.getInstance();
		List<CastMovieTO> movieTOs=castTO.getCastMovieList();
		MovieDAO movieDao=new MovieDAO();
		for(CastMovieTO castMovieTO : movieTOs){
			db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(),ConstantsHBase.FAMILY_CAST_MOVIE , ConstantsHBase.QUALIFIER_CAST_MOVIE_ID,castMovieTO.getId());

			db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(),ConstantsHBase.FAMILY_CAST_MOVIE , ConstantsHBase.QUALIFIER_CAST_CHARACTER, castMovieTO.getCharacter());
			db.put(ConstantsHBase.TABLE_CAST, castTO.getId()+"_"+castMovieTO.getId(),ConstantsHBase.FAMILY_CAST_MOVIE , ConstantsHBase.QUALIFIER_CAST_ORDER, castMovieTO.getOrder());
			
			movieDao.insertMovieCast(castTO,castMovieTO.getId());
		}
		
	}

	public CastTO getCrewById(int castId) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CAST);
		Get get = new Get(Bytes.toBytes(castId));
		CastTO castTO = new CastTO();
		try {
			Result result = table.get(get);
			if(!result.isEmpty()){
				castTO.setId(castId);
				castTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_NAME))));
			}
			} catch (IOException e) {
			e.printStackTrace();
		}
		return castTO;
	}
	
	
	
    /**
     * This method returns all the movies that Cast worked in.
     * @param castId
     * @return List of MovieTO
     */
    public List<MovieTO> getMoviesByCast(int castId) {
    	HBaseDB hBaseDB = HBaseDB.getInstance();
    	Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CAST);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(castId+"_"));
		scan.setFilter(filter);
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
        List<MovieTO> movieList = new ArrayList<MovieTO>();
       // CastTO castTO = new CastTO();
        MovieTO movieTO = null;
    	if(resultScanner != null){
    		Iterator<Result> iter = resultScanner.iterator();
    		MovieDAO movieDAO = new MovieDAO();
    		while(iter.hasNext()){
				Result result = iter.next();
				if(result!=null&!result.isEmpty()){
					int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CAST_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CAST_MOVIE_ID)));
					movieTO = movieDAO.getMovieById(movieId);
				    movieList.add(movieTO);
				}
			}		
    	}
        return movieList;

    }
}
