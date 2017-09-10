package oracle.demo.oow.bd.dao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.to.CastCrewTO;
import oracle.demo.oow.bd.to.CastTO;
import oracle.demo.oow.bd.to.CrewTO;
import oracle.demo.oow.bd.to.CustomerTO;
import oracle.demo.oow.bd.to.GenreTO;
import oracle.demo.oow.bd.to.MovieTO;
import oracle.demo.oow.bd.util.StringUtil;
import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;

public class MovieDAO{
    public void insertMovie(MovieTO movieTO) {
    	HBaseDB db = HBaseDB.getInstance();
    	db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE,movieTO.getTitle());
        db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW,movieTO.getOverview());
        db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_POSTER_PATH,movieTO.getPosterPath());
        db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE,movieTO.getDate());
        db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT,movieTO.getVoteCount());
        db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_RUNTIME,movieTO.getRunTime());
        db.put(ConstantsHBase.TABLE_MOVIE,movieTO.getId(),ConstantsHBase.FAMILY_MOVIE_MOVIE,ConstantsHBase.QUALIFIER_MOVIE_POPULARITY,movieTO.getPopularity());

        try{
      	  insertMovieGenres(movieTO);
        }	catch(Exception e){
      	  e.printStackTrace();
        }
    }

    //映射
	private void insertMovieGenres(MovieTO movieTO) {
		HBaseDB db=HBaseDB.getInstance();
		List<GenreTO> genreTOs=movieTO.getGenres();
		GenreDAO genreDao=new GenreDAO();
		for(GenreTO genreTO : genreTOs){
			db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId()+"_"+genreTO.getId(),ConstantsHBase.FAMILY_MOVIE_GENRE , ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID,genreTO.getId());
			db.put(ConstantsHBase.TABLE_MOVIE, movieTO.getId()+"_"+genreTO.getId(),ConstantsHBase.FAMILY_MOVIE_GENRE , ConstantsHBase.QUALIFIER_MOVIE_GENRE_NAME, genreTO.getName());
			//if(!genreDao.isExists(genreTO)){
				genreDao.insertGenre(genreTO);
			//}
			genreDao.insertGenreMovie(movieTO,genreTO);
		}
		
	}

	//movie和cast的映射
	public void insertMovieCast(CastTO castTO, int id) {
		// TODO Auto-generated method stub
		HBaseDB db=HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE, id+"_"+castTO.getId(), ConstantsHBase.FAMILY_MOVIE_CAST, ConstantsHBase.QUALIFIER_MOVIE_CAST_ID, castTO.getId());
	}
	
	public void insertMovieCrew(CrewTO crewTO, int id) {
		// TODO Auto-generated method stub
		HBaseDB db=HBaseDB.getInstance();
		db.put(ConstantsHBase.TABLE_MOVIE, id+"_"+crewTO.getId(), ConstantsHBase.FAMILY_MOVIE_CREW, ConstantsHBase.QUALIFIER_MOVIE_CREW_ID, crewTO.getId());
	}
  
	/*
	 * 
	 * 
	 *获取电影的基本的信息
	 */
	public MovieTO getMovieById(int id) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Get get = new Get(Bytes.toBytes(id));
		MovieTO movieTO = new MovieTO();
		try {
			Result result = table.get(get);
			if(!result.isEmpty()){
		    movieTO.setId(id);
			movieTO.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_ORIGINAL_TITLE))));
			movieTO.setOverview(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_OVERVIEW))));
			movieTO.setPosterPath(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
			movieTO.setDate(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RELEASE_DATE))));
			movieTO.setVoteCount(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_VOTE_COUNT))));
			movieTO.setRunTime(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_RUNTIME))));
			movieTO.setPopularity(Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_POPULARITY))));
		     }
			} catch (IOException e) {
			e.printStackTrace();
		}
		return movieTO;
	}
	
	
	
	//获取电影详细信息
	public MovieTO getMovieDetailById(int movieId){
		//设置genre
		MovieTO movieTO = getMovieById(movieId);
		ArrayList<GenreTO> genres  = getGenresByMovieId(movieId);
		movieTO.setGenres(genres);
		//设置cast
		CastCrewTO castCrewTO = new CastCrewTO();
		List<CastTO> castList = getCastByMovieId(movieId);
		castCrewTO.setCastList(castList);
		//设置crew
		List<CrewTO> crewList = getCrewByMovieId(movieId);
		castCrewTO.setCrewList(crewList);
		
		movieTO.setCastCrewTO(castCrewTO);
		return movieTO;
		
	}

	private List<CrewTO> getCrewByMovieId(int movieId) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW));
		scan.setFilter(filter);
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<CrewTO> crewTOs  =new ArrayList();
		CrewTO crewTO = new CrewTO();
		if(resultScanner != null){
			Iterator<Result> iter = resultScanner.iterator();
			CrewDAO crewDAO = new  CrewDAO();
			while(iter.hasNext()){
				Result result = iter.next();
				if(result!=null&!result.isEmpty()){
					int crewId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CREW_ID)));
				    crewTO =  crewDAO.getCrewById(crewId);
				}
				crewTOs.add(crewTO);
			}				
		}
		return crewTOs;
	}

	private List<CastTO> getCastByMovieId(int movieId) {	 
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST));
		scan.setFilter(filter);
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		List<CastTO> castTOs  =new ArrayList();
		CastTO castTO = new CastTO();
		if(resultScanner != null){
			Iterator<Result> iter = resultScanner.iterator();
			CastDAO castDAO = new  CastDAO();
			while(iter.hasNext()){
				Result result = iter.next();
				if(result!=null&!result.isEmpty()){
					int castId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_CAST), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_CAST_ID)));
				    castTO =  castDAO.getCrewById(castId);
				}
				castTOs.add(castTO);
			}				
		}
		return castTOs;
	}

	private ArrayList<GenreTO> getGenresByMovieId(int movieId) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_MOVIE);
		Filter filter = new PrefixFilter(Bytes.toBytes(movieId+"_"));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE));
		scan.setFilter(filter);
		ResultScanner resultScanner = null;
		try {
			resultScanner = table.getScanner(scan);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ArrayList<GenreTO> genreTos  =new ArrayList();
		GenreTO genreTO = null;
		if(resultScanner != null){
			Iterator<Result> iter = resultScanner.iterator();
			GenreDAO genreDAO = new GenreDAO();
			while(iter.hasNext()){
				Result result = iter.next();
				if(result!=null&!result.isEmpty()){
					int genreId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_MOVIE_GENRE), Bytes.toBytes(ConstantsHBase.QUALIFIER_MOVIE_GENRE_ID)));
				    genreTO =  genreDAO.getGenreById(genreId);
				}
				genreTos.add(genreTO);
			}				
		}
		return genreTos;
			
	}

	public List<MovieTO> getMoviesCurrentWatchList(int custId, int activity) throws IOException {
		  HBaseDB db = HBaseDB.getInstance();
	        Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
	        Scan scan=new Scan();
	        Filter filter = new SingleColumnValueFilter(
			Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_USER_ID),
					CompareFilter.CompareOp.EQUAL, Bytes.toBytes(custId));
	    	((SingleColumnValueFilter) filter).setFilterIfMissing(true);  
	    	 Filter filter2 = new SingleColumnValueFilter(
	    				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
	    						CompareFilter.CompareOp.EQUAL, Bytes.toBytes(activity));
	    		    	((SingleColumnValueFilter) filter2).setFilterIfMissing(true);  
	        FilterList filterList = new FilterList();
	        filterList.addFilter(filter);
	    	filterList.addFilter(filter2);
	    	scan.setFilter(filterList);	    	
			ResultScanner resultScanner = table.getScanner(scan);
			MovieTO movieTO = new MovieTO();
	    	List<MovieTO> movieList = new ArrayList<MovieTO>();	    
	    	if(resultScanner != null){
	    		Iterator<Result> iter = resultScanner.iterator();
	    		MovieDAO movieDAO = new MovieDAO();
	    		while(iter.hasNext()){
					Result result = iter.next();
					if(result!=null&!result.isEmpty()){
						int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
						movieTO = movieDAO.getMovieById(movieId);
					    movieList.add(movieTO);
					}
				}		
	    	}
	        return movieList;
	}

	public List<MovieTO> getCommonPlayList(int activity) throws Exception {
		 HBaseDB db = HBaseDB.getInstance();
	        Table table = db.getTable(ConstantsHBase.TABLE_ACTIVITY);
	        Scan scan=new Scan();
	    	 Filter filter2 = new SingleColumnValueFilter(
	    				Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_ACTIVITY),
	    						CompareFilter.CompareOp.EQUAL, Bytes.toBytes(activity));
	    		    	((SingleColumnValueFilter) filter2).setFilterIfMissing(true);  
	    	scan.setFilter(filter2);	    	
			ResultScanner resultScanner = table.getScanner(scan);
			MovieTO movieTO = new MovieTO();
	    	List<MovieTO> movieList = new ArrayList<MovieTO>();	    
	    	if(resultScanner != null){
	    		Iterator<Result> iter = resultScanner.iterator();
	    		MovieDAO movieDAO = new MovieDAO();
	    		while(iter.hasNext()){
					Result result = iter.next();
					if(result!=null&!result.isEmpty()){
						int movieId = Bytes.toInt(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY), Bytes.toBytes(ConstantsHBase.QUALIFIER_ACTIVITY_MOVIE_ID)));
						System.out.println(movieId);
						movieTO = movieDAO.getMovieById(movieId);
					    movieList.add(movieTO);
					}
				}		
	    	}
	        return movieList;
	}
}
