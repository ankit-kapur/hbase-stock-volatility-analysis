import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Job2 {

	public static class MyMapper extends TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			String stockName = new String(result.getValue("stock".getBytes(),
					"name".getBytes()));
			String yr = new String(result.getValue("time".getBytes(),
					"yr".getBytes()));
			String mm = new String(result.getValue("time".getBytes(),
					"mm".getBytes()));
			String dd = new String(result.getValue("time".getBytes(),
					"dd".getBytes()));
			String adjClose = new String(result.getValue("price".getBytes(),
					"price".getBytes()));

			String key = stockName + "#" + yr + "#" + mm;
			String value = dd + "#" + adjClose;

			context.write(new Text(key), new Text(value));
		}
	}

	/* Reducer */
	public static class MyTableReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			String firstDayPrice = null, lastDayPrice = null;
			try {
				int firstDay = 32;
				int lastDay = 0;

				for (Text result : iterable) {

					String[] splits = result.toString().split("#");
					int day = Integer.parseInt(splits[0]);
					String price = splits[1];

					if (day < firstDay) {
						firstDay = day;
						firstDayPrice = price;
					}
					if (day > lastDay) {
						lastDay = day;
						lastDayPrice = price;
					}
				}

				/*
				 * Now, we've found the data for the 1st and last day of the
				 * month. Calculate xi
				 */
				double adjBegin = Double.parseDouble(firstDayPrice);
				double adjEnd = Double.parseDouble(lastDayPrice);

				String[] splitKey = key.toString().split("#");
				byte[] stockName = Bytes.toBytes(splitKey[0]);
				byte[] yr = Bytes.toBytes(splitKey[1]);
				byte[] mm = Bytes.toBytes(splitKey[2]);

				/* Apply the formula for xi */
				double xi = (adjEnd - adjBegin) / adjBegin;
//				DecimalFormat df = new DecimalFormat("0.0");
//				df.setMaximumFractionDigits(10);
//				String xiString = df.format(xi);
				String xiString = String.valueOf(xi);

				byte[] rowId = Bytes.toBytes(key.toString());
				Put put = new Put(rowId);
				put.add(Bytes.toBytes("stock"), Bytes.toBytes("name"),
						stockName);
				put.add(Bytes.toBytes("time"), Bytes.toBytes("yr"), yr);
				put.add(Bytes.toBytes("time"), Bytes.toBytes("mm"), mm);
				put.add(Bytes.toBytes("price"), Bytes.toBytes("xi"),
						xiString.getBytes());

				context.write(new ImmutableBytesWritable(rowId), put);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}