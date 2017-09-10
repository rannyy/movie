package oracle.demo.oow.bd.util.Hbase;

import oracle.demo.oow.bd.dao.hbase.CustomerDao;
import oracle.demo.oow.bd.to.CustomerTO;

public class CustomerTest {

	public static void main(String[] args) {
		CustomerDao userDao = new CustomerDao();
		
		CustomerTO customerTO = userDao.getCustomerByCredential("guest1", "welcome1");
		System.out.println(customerTO.getUserName()+","+customerTO.getId()+","+customerTO.getPassword());
	}
}
