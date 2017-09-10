package oracle.demo.oow.bd.util.Hbase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class DBUtil {
	static String db_username = ConstantsHBase.MYSQL_USERNAME;
	static String db_passsword =ConstantsHBase.MYSQL_PASSWORD ;
	static String URL = ConstantsHBase.MYSQL_URL;
	static String DRIVER = ConstantsHBase.MYSQL_DRIVER;
	private Connection conn = null;
	private static Statement stmt = null;
	
	
	public static Connection getConn(){
		Connection conn= null;
		try
		{
		Class.forName(DRIVER);
		System.out.println("加载驱动成功！");
		}catch(Exception e){
		e.printStackTrace();
		System.out.println("加载驱动失败！");
		}
		try{
	    conn =DriverManager.getConnection(URL,db_username,db_passsword);
		System.out.println("连接数据库成功！");
		stmt = conn.createStatement();
		}catch(Exception e)
		{
		e.printStackTrace();
		System.out.print("SQL Server连接失败！");
		} 
		return conn;
	}

	public int executeUpdate(String s) {
		int result = 0;
		try {
			result = stmt.executeUpdate(s);
		} catch (Exception ex) {
			System.out.println("执行更新错误！");
			ex.printStackTrace();
		}
		return result;
	}

	public ResultSet executeQuery(String s) {
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(s);
			System.out.println("执行查询正确！");
		} catch (Exception ex) {
			System.out.println("执行查询错误！");
		}
		return rs;
	}

	public void close() {
		try {
			stmt.close();
			conn.close();
		} catch (Exception e) {
		}
	}
	
}
