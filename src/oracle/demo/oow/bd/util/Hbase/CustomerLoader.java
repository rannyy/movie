package oracle.demo.oow.bd.util.Hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.dao.hbase.CustomerDao;
import oracle.demo.oow.bd.to.CustomerTO;


public class CustomerLoader {
	
	public static void main(String[] args) {
		CustomerLoader loader = new CustomerLoader();
		try {
			loader.uploadProfile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void uploadProfile() throws IOException {
        FileReader fr = null;
        CustomerDao customerDao = new CustomerDao();
        try {
                        
            fr = new FileReader(Constant.CUSTOMER_PROFILE_FILE_NAME);
            BufferedReader br = new BufferedReader(fr);
            String jsonTxt = null;
            String password = Constant.DEMO_PASSWORD;
            CustomerTO custTO = null;
            int count = 1;
            
            while ((jsonTxt = br.readLine()) != null) {
                
                if (jsonTxt.trim().length() == 0)
                    continue;
                
                try {
                    custTO = new CustomerTO(jsonTxt.trim());
                    
                    //Set password to each CutomerTO
                    custTO.setPassword(password);
                } catch (Exception e) {
                    System.out.println("ERROR: Not able to parse the json string: \t" + jsonTxt);
                }

                if (custTO != null) {                   
                	   customerDao.insert(custTO);          
                       System.out.println(count++ + " " +custTO.getJsonTxt());

                } //EOF if

            } //EOF while
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            fr.close();
        }
    }

}
