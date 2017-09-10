package oracle.demo.oow.bd.util.Hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CrewDAO;
import oracle.demo.oow.bd.to.CrewTO;



public class MovieCrewLoader {
    public static void main(String[] args) {
    	MovieCrewLoader movieCrewLoader = new MovieCrewLoader();
		try {
			movieCrewLoader.uploadProfile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    public void uploadProfile() throws IOException {
        FileReader fr = null;
        CrewDAO crewDAO = new CrewDAO();
        try {
            //fr = new FileReader(Constant.MOVIE_CASTS_FILE_NAME);
            fr = new FileReader(Constant.MOVIE_CREWS_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            //String password = StringUtil.getMessageDigest(Constant.DEMO_PASSWORD);
            CrewTO crewTO = null;
            int count = 1;
            while ((jsonTxt = br.readLine()) != null) {                          
                try {
                	crewTO = new CrewTO(jsonTxt.trim());
                    
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" +
                                       jsonTxt);
                }
                if (crewTO != null) {
                    crewDAO.insertCrewInfo(crewTO);
                    System.out.println(count++ + " "+ crewTO.getJsonTxt());
                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
    } //uploadProfile

}
