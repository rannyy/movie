package oracle.demo.oow.bd.dao.hbase;


import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;

import java.util.List;

import oracle.demo.oow.bd.dao.hbase.MovieDAO;

import oracle.demo.oow.bd.to.CrewTO;


import oracle.demo.oow.bd.to.MovieTO;

import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;




import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CrewDAO{

    public void insertCrewInfo(CrewTO crewTO) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		hBaseDB.put(ConstantsHBase.TABLE_CREW, crewTO.getId(), ConstantsHBase.FAMILY_CREW_CREW, ConstantsHBase.QUALIFIER_CREW_JOB, crewTO.getJob());
		hBaseDB.put(ConstantsHBase.TABLE_CREW, crewTO.getId(), ConstantsHBase.FAMILY_CREW_CREW, ConstantsHBase.QUALIFIER_CAST_NAME, crewTO.getName());
	    
		insertCrewToMovie(crewTO);
		
    }

	private void insertCrewToMovie(CrewTO crewTO) {
		HBaseDB db=HBaseDB.getInstance();
		List<String> movieTOs=crewTO.getMovieList();
		MovieDAO movieDao=new MovieDAO();
		for(String movieId : movieTOs){
			db.put(ConstantsHBase.TABLE_CREW, crewTO.getId()+"_"+movieId,ConstantsHBase.FAMILY_CREW_MOVIE , ConstantsHBase.QUALIFIER_CREW_MOVIE_ID,movieId);
			movieDao.insertMovieCrew(crewTO,Integer.valueOf(movieId));
		}
		
	}

	public CrewTO getCrewById(int crewId) {
		HBaseDB hBaseDB = HBaseDB.getInstance();
		Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CREW);
		Get get = new Get(Bytes.toBytes(crewId));
		CrewTO crewTO = new CrewTO();
		try {
			Result result = table.get(get);
			if(!result.isEmpty()){
				crewTO.setId(crewId);
				crewTO.setName(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_NAME))));
				crewTO.setJob(Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_CREW), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_JOB))));
			}
			} catch (IOException e) {
			e.printStackTrace();
		}
		return crewTO;
	}

	 /**
     * This method returns all the movies that Crew worked in.
     * @param crewId
     * @return List of MovieTO
     */
    public List<MovieTO> getMoviesByCrew(int crewId) {
    	HBaseDB hBaseDB = HBaseDB.getInstance();
    	Table table = hBaseDB.getTable(ConstantsHBase.TABLE_CREW);
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE));
		Filter filter = new PrefixFilter(Bytes.toBytes(crewId+"_"));
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
					String movieId = Bytes.toString(result.getValue(Bytes.toBytes(ConstantsHBase.FAMILY_CREW_MOVIE), Bytes.toBytes(ConstantsHBase.QUALIFIER_CREW_MOVIE_ID)));
					movieTO = movieDAO.getMovieById(Integer.valueOf(movieId));
				    movieList.add(movieTO);
				}
			}		
    	}
        return movieList;
    }
}
