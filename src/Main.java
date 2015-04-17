import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Main {

	public static void main(String[] args) {

		String filepath = "/home/ankitkap/dic/data/small/*.csv";
		long startTime = new Date().getTime();
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = null;
		HTable hTable = null;
		try {
			admin = new HBaseAdmin(config);
			String table1Name = "raw";
			String table2Name = "xitable";
			String table3Name = "voltable";
			String table4Name = "sortedtable";
			String[] tableList = new String[] { table1Name, table2Name,
					table3Name, table4Name };

			deleteTablesIfTheyExist(tableList, admin);
			createTables(tableList, admin);

			/* =========== 1st job */
			System.out.println("Starting Job 1.");
			Job job1 = Job.getInstance();
			job1.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job1, new Path(filepath));
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob(table1Name, null, job1);
			job1.setNumReduceTasks(0);
			job1.waitForCompletion(true);
			long job1Time = new Date().getTime();
			System.out.println("Job 1 complete. Time taken ==> " + (job1Time - startTime) / 1000.0 + " seconds\n");

			/* =========== 2nd job */
			System.out.println("Starting Job 2.");
			Job job2 = Job.getInstance();
			job2.setJarByClass(Main.class);
			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob(table1Name, scan,
					Job2.MyMapper.class, Text.class, Text.class, job2);
			TableMapReduceUtil.initTableReducerJob(table2Name,
					Job2.MyTableReducer.class, job2);
			boolean b = job2.waitForCompletion(true);
			long job2Time = new Date().getTime();
			System.out.println("Job 2 complete. Time taken ==> " + (job2Time - job1Time) / 1000.0 + " seconds\n");
			if (!b)
				System.err.println("Failure in Job 2");

			/* =========== 3rd job */
			System.out.println("Starting Job 3.");
			Job job3 = Job.getInstance();
			job3.setJarByClass(Main.class);
			scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob(table2Name, scan,
					Job3.MyMapper.class, Text.class, Text.class, job3);
			TableMapReduceUtil.initTableReducerJob(table3Name,
					Job3.MyTableReducer.class, job3);
			b = job3.waitForCompletion(true);
			long job3Time = new Date().getTime();
			System.out.println("Job 3 complete. Time taken ==> " + (job3Time - job2Time) / 1000.0 + " seconds\n");
			if (!b)
				System.err.println("Failure in Job 3");
			/* Get the value of the stock counter */
			Counter stockCounter = job3.getCounters().findCounter(
					STOCK_COUNTER.NUM_OF_STOCKS);
			long numOfStocks = stockCounter.getValue();

			/* =========== 4th job */
			System.out.println("Starting Job 4.");
			Job job4 = Job.getInstance();
			job4.setJarByClass(Main.class);
			scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob(table3Name, scan,
					Job4.MyMapper.class, Text.class, Text.class, job4);
			TableMapReduceUtil.initTableReducerJob(table4Name,
					Job4.MyTableReducer.class, job4);
			job4.setNumReduceTasks(1);
			job4.getConfiguration().set("numOfStocks",
					Long.toString(numOfStocks));
			b = job4.waitForCompletion(true);
			long job4Time = new Date().getTime();
			System.out.println("Job 4 complete. Time taken ==> " + (job4Time - job3Time) / 1000.0 + " seconds\n");
			if (!b)
				System.err.println("Failure in Job 4");

			hTable = new HTable(config, table4Name);
			Map<String, List<String>> sortedMap = new TreeMap<>();
			Scan scan4 = new Scan();
			ResultScanner resultScanner = hTable.getScanner(scan4);
			for(Result scannerResult:resultScanner)
			{
				String rowKey = Bytes.toString(scannerResult.getRow());
				String stockName=new String(scannerResult.getValue(Bytes.toBytes("stock"), Bytes.toBytes("name")));
				String volatility=new String(scannerResult.getValue(Bytes.toBytes("price"), Bytes.toBytes("volatility")));
				List<String> list = new ArrayList<String>();
				list.add(stockName);
				list.add(volatility);
				sortedMap.put(rowKey, list);
			}
			
			System.out.println("\n\n..:: Top 10 and bottom 10 stocks ::..");
			for (String key: sortedMap.keySet())
				System.out.println(sortedMap.get(key).get(0) + " ==> " + sortedMap.get(key).get(1));
			
			/* Record the time taken */
			long endTime = new Date().getTime();
			System.out.println("\nTime taken for all jobs to complete: " + (endTime - startTime) / 1000.0 + " seconds");
			System.out.println("-------=== Completed --> Stock volatility estimation ===------- \n");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null)
					admin.close();
				if (hTable != null)
					hTable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private static void createTables(String[] tableNames, HBaseAdmin admin)
			throws IOException {
		for (String tableName : tableNames) {
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			tableDescriptor.addFamily(new HColumnDescriptor("stock"));
			tableDescriptor.addFamily(new HColumnDescriptor("time"));
			tableDescriptor.addFamily(new HColumnDescriptor("price"));
			admin.createTable(tableDescriptor);
		}
	}

	private static void deleteTablesIfTheyExist(String[] tableNames,
			HBaseAdmin admin) throws IOException {
		for (String tableName : tableNames) {
			if (admin.isTableAvailable(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
		}
	}

	/* The counter for number of stocks */
	public static enum STOCK_COUNTER {
		NUM_OF_STOCKS,
	};

	/* A utility function */
	public static boolean isNumeric(String string) {
		if (string == null)
			return false;
		try {
			Integer.parseInt(string);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	/* A utility function */
	public static boolean isDouble(String string) {
		if (string == null)
			return false;
		try {
			Double.parseDouble(string);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}
}