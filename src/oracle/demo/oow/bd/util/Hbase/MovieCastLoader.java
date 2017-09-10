package oracle.demo.oow.bd.util.Hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CastDAO;
import oracle.demo.oow.bd.to.CastTO;



public class MovieCastLoader {
    public static void main(String[] args) {
    	MovieCastLoader movieCastLoader = new MovieCastLoader();
		try {
			movieCastLoader.uploadProfile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    public void uploadProfile() throws IOException {
        FileReader fr = null;
        CastDAO castDAO = new CastDAO();
        try {
            //fr = new FileReader(Constant.MOVIE_CASTS_FILE_NAME);
            fr = new FileReader(Constant.MOVIE_CASTS_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            //String password = StringUtil.getMessageDigest(Constant.DEMO_PASSWORD);
            CastTO castTO = null;
            int count = 1;
            while ((jsonTxt = br.readLine()) != null) {                           
                try {
                	castTO = new CastTO(jsonTxt.trim());
                    
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }
                if (castTO != null) {
                   castDAO.insertCastInfo(castTO);
                    System.out.println(count++ + " " + castTO.getJsonTxt());
                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
    } //uploadProfile

}
