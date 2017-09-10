package oracle.demo.oow.bd.dao.hbase;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import oracle.demo.oow.bd.constant.Constant;
import oracle.demo.oow.bd.util.Hbase.ConstantsHBase;
import oracle.demo.oow.bd.util.Hbase.DBUtil;
import oracle.demo.oow.bd.util.Hbase.HBaseDB;

/**
 * This class is used to access recommended movie data for customer
 */
public class CustomerRatingDAO{

	    public void insertCustomerRating(int userId, int movieId, int rating) {
	        String insert = null;
	        PreparedStatement stmt = null;
	        Connection conn = DBUtil.getConn();

	        insert =
	                "INSERT INTO cust_rating (USERID, MOVIEID, RATING)  VALUES (?, ?, ?)";
	        try {
	            if (conn != null) {
	                stmt = conn.prepareStatement(insert);
	                stmt.setInt(1, userId);
	                stmt.setInt(2, movieId);
	                stmt.setInt(3, rating);
	                stmt.execute();
	                stmt.close();
	                System.out.println("INFO: Customer: " + userId + " Movie: " +
	                                   movieId + " rating: " + rating);
	            }

	        } catch (SQLException e) {
	            System.out.println(e.getErrorCode() + ":" + e.getMessage());
	        }
	    }

}
