import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Job3 {

	public static class MyMapper extends TableMapper<Text, Text> {

		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			String stockName = new String(result.getValue("stock".getBytes(),
					"name".getBytes()));
			String xi = new String(result.getValue("price".getBytes(),
					"xi".getBytes()));

			String key = stockName;
			String value = xi;

			context.write(new Text(key), new Text(value));
		}
	}

	/* Reducer */
	public static class MyTableReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			try {
				double temp = 0.0, volatility = 0.0;
				List<Double> xiList = new ArrayList<>();
				int N = 0;
				double xbar = 0.0;
				for (Text xiString : iterable) {
					double xi = Double.parseDouble(xiString.toString());
					xbar += xi;
					N++;
					xiList.add(xi);
				}

				if (N > 0 && N - 1 > 0) {
					xbar /= N;

					for (double xi : xiList)
						temp += Math.pow((xi - xbar), 2);

					volatility = Math.sqrt(temp / (N - 1));

					if (volatility < 0.0 || volatility > 0.0) {
						/* Apply the formula for xi */
						DecimalFormat df = new DecimalFormat("0.0");
						df.setMaximumFractionDigits(10);
						String volatilityString = df.format(volatility);

						byte[] rowId = Bytes.toBytes(key.toString());
						Put put = new Put(rowId);
						put.add(Bytes.toBytes("price"),
								Bytes.toBytes("volatility"),
								volatilityString.getBytes());
						put.add(Bytes.toBytes("stock"), Bytes.toBytes("name"),
								key.toString().getBytes());

						/* ----- Increment the stock counter ----- */
						context.getCounter(Main.STOCK_COUNTER.NUM_OF_STOCKS).increment(1);
						context.write(new ImmutableBytesWritable(rowId), put);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}