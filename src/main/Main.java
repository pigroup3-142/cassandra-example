package main;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import model.ModelLog;
import parquet.ParquetData;

public class Main {
	public static void main(String[] args) {
		ParquetData parquetData = new ParquetData();
//		Dataset<Row> listDataset = parquetData.getListDataset("hdfs://localhost:9001/usr/trannguyenhan/pageviewlog");

//		Map<String, String> map = new HashedMap();
//		map.put("table", "pageviewlog");
//		map.put("keyspace", "trannguyenhan");
//		
//		listDataset.write()
//		   .format("org.apache.spark.sql.cassandra")
//		   .options(map)
//		   .save();
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark-Cassandra example");
		sparkConf.setMaster("local[2]");
		sparkConf.set("spark.cassandra.connection.host", "127.0.0.1");
		sparkConf.set("spark.cassandra.connection.native.port", "9042");
		sparkConf.set("spark.cassandra.connection.rpc.port", "9160");
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<ModelLog> listModelLog = parquetData.getListModelLog("hdfs://localhost:9001/usr/trannguyenhan/pageviewlog");
		JavaRDD<ModelLog> listModelLogRDD = sc.parallelize(listModelLog);
		
		CassandraJavaUtil.javaFunctions(listModelLogRDD).writerBuilder("trannguyenhan", "pageviewlog", CassandraJavaUtil.mapToRow(ModelLog.class)).saveToCassandra();

		
	}
}
