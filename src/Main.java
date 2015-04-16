import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Main {

	public static void main(String[] args) {

		String filepath = "/home/ankitkap/dic/data/veryverysmall/*.csv";
		Configuration conf = HBaseConfiguration.create();
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
			String rawTableName = "raw";
			String table2Name = "xitable";
			String[] tableList = new String[] { rawTableName, table2Name };

			deleteTablesIfTheyExist(tableList, admin);
			createTables(tableList, admin);

			Job job1 = Job.getInstance();
			job1.setJarByClass(Main.class);
			FileInputFormat.addInputPath(job1, new Path(filepath));
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setMapperClass(Job1.Map.class);
			TableMapReduceUtil.initTableReducerJob(rawTableName, null, job1);
			job1.setNumReduceTasks(0);
			job1.waitForCompletion(true);
			System.out.println("Job 1 complete.");

			/* =========== 2nd job */
			System.out.println("Starting Job 2.");
			Job job2 = Job.getInstance();
			job2.setJarByClass(Main.class); // class that contains mapper and reducer

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);

			TableMapReduceUtil.initTableMapperJob(rawTableName, // input table
					scan, // Scan instance to control CF and attribute selection
					Job2.MyMapper.class, // mapper class
					Text.class, // mapper output key
					Text.class, // mapper output value
					job2);

			TableMapReduceUtil.initTableReducerJob(table2Name, // output table
					Job2.MyTableReducer.class, // reducer class
					job2);
//			job2.setNumReduceTasks(1); // At least one, adjust as required

			boolean b = job2.waitForCompletion(true);
			System.out.println("Job 2 complete.");
			if (!b)
				System.err.println("Failed");
//				throw new IOException("Error with job2!");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (admin != null)
					admin.close();
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
}