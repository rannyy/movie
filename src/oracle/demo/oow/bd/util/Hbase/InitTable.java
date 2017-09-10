package oracle.demo.oow.bd.util.Hbase;

/**
 * 创建对应的hbase表结构
 * @author Administrator
 *
 */
public class InitTable {
public static void main(String[] args) {
		
		/*HBaseDB db = HBaseDB.getInstance();
		String tableName = "CUSTOMER";
		String[] columnFamilies = {"info", "id"};
		db.createTable(tableName, columnFamilies);*/
		
		
		HBaseDB db = HBaseDB.getInstance();
		String table_activity = ConstantsHBase.TABLE_ACTIVITY;
		String[] fam_activity = {ConstantsHBase.FAMILY_ACTIVITY_ACTIVITY};
		db.createTable(table_activity, fam_activity);
		//gid表，计数器表，用于生成行建
		/*String table_gid = ConstantsHBase.TABLE_GID;
		String[] columnFamilies_gid = {ConstantsHBase.FAMILY_GID_GID};
		db.createTable(table_gid, columnFamilies_gid);*/
		
		/*String table_movie = ConstantsHBase.TABLE_MOVIE;
		String[] columnFamilies_movie = {ConstantsHBase.FAMILY_MOVIE_CAST,ConstantsHBase.FAMILY_MOVIE_CREW,ConstantsHBase.FAMILY_MOVIE_GENRE,ConstantsHBase.FAMILY_MOVIE_MOVIE};
		db.createTable(table_movie, columnFamilies_movie);*/
		
		
		
		/*String table_crew = ConstantsHBase.TABLE_CREW;
		String[] columnFamilies_crew = {ConstantsHBase.FAMILY_CREW_CREW,ConstantsHBase.FAMILY_CREW_MOVIE};
		db.createTable(table_crew, columnFamilies_crew);*/
		
		/*String table_genre = ConstantsHBase.TABLE_GENRE;
		String[] columnFamilies_genre = {ConstantsHBase.FAMILY_GENRE_GENRE,ConstantsHBase.FAMILY_GENRE_MOVIE};
		db.createTable(table_genre, columnFamilies_genre);*/
		
		/*String table_cast = ConstantsHBase.TABLE_CAST;
		String[] columnFamilies_cast = {ConstantsHBase.FAMILY_CAST_CAST,ConstantsHBase.FAMILY_CAST_MOVIE};
		db.createTable(table_cast, columnFamilies_cast);*/
		
	}

}
