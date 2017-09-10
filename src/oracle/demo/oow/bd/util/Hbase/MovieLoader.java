package oracle.demo.oow.bd.util.Hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.MovieDAO;



import oracle.demo.oow.bd.to.MovieTO;


public class MovieLoader {

    public void uploadMovieInfo() throws IOException {
        FileReader fr = null;
        try {
            fr = new FileReader(Constant.MOVIE_INFO_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            MovieTO movieTO = null;
            MovieDAO movieDAO = new MovieDAO();
            int count = 1;
            while ((jsonTxt = br.readLine()) != null) {         
                    try {
                        movieTO = new MovieTO(jsonTxt.trim());
                    } catch (Exception e) {
                        System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
                    }

                    if (movieTO != null && !movieTO.isAdult()) {   
                    	    System.out.println(count++ + " " +movieTO.getMovieJsonTxt());
                            movieDAO.insertMovie(movieTO);

                    }
                }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }


    } //uploadMovies


   

    public static void main(String[] args) throws Exception {
        MovieLoader mu = new MovieLoader();
        mu.uploadMovieInfo();
    } //main

}
