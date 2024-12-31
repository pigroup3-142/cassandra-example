package parquet;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import model.ModelLog;

public class ParquetData {
	private SparkSession spark;

	public ParquetData() {
		spark = SparkSession.builder().appName("Read file Parquet from HDFS").master("local").getOrCreate();
	}

	/* method get data from parquet file, return List<Object>
	 * */
	public List<ModelLog> getListModelLog(String pathFile) {
		Dataset<Row> parquetFile = spark.read().parquet(pathFile);
		parquetFile.printSchema();

		List<ModelLog> listModelLog = new ArrayList<ModelLog>();
		List<Row> listRow = parquetFile.collectAsList();
		for (Row row : listRow) {
			Date timeCreate = new Date(20);
			Date cookieCreate = new Date(4);
			int browserCode = row.getInt(0);
			String browserVer = row.getString(1);
			int osCode = row.getInt(12);
			String osVer = row.getString(14);
			long ip = row.getLong(9);
			int locId = row.getInt(11);
			String domain = row.getString(5);
			int siteId = row.getInt(18);
			int cId = row.getInt(2);
			String path = row.getString(15);
			String referer = row.getString(16);
			long guid = row.getLong(8);
			String flashVersion = row.getString(6);
			String jre = row.getString(10);
			String sr = row.getString(19);
			String sc = row.getString(17);
			int geographic = row.getInt(7);
			String category = row.getString(3);
			String osName = row.getString(13);

			// create object ModelLog from attribute above
			ModelLog model = new ModelLog(timeCreate, browserCode, browserVer, osName, osCode, osVer, ip, domain, path,
					cookieCreate, guid, siteId, cId, referer, geographic, locId, flashVersion, jre, sr, sc, category);

			// print object and check
			System.out.println(model);
			listModelLog.add(model);
			
		}
		
		return listModelLog;
	}
	
	/* method get data from parquet file, return Dataset<Row>
	 * */
	public Dataset<Row> getListDataset(String pathFile){
		Dataset<Row> parquetFile = spark.read().parquet(pathFile);
		parquetFile.printSchema();
		
		return parquetFile;
	}
	
}
