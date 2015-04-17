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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

public class Job4 {

	static int counter=1;
	public static class MyMapper extends TableMapper<DoubleWritable, Text> {
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			String stockName = new String(result.getValue("stock".getBytes(),
					"name".getBytes()));
			String volatility = new String(result.getValue("price".getBytes(),
					"volatility".getBytes()));

			String key = volatility;
			String value = stockName;

			context.write(new DoubleWritable(Double.parseDouble(key)), new Text(value));
		}
	}

	/* Reducer */
	public static class MyTableReducer extends
			TableReducer<DoubleWritable, Text, ImmutableBytesWritable> {

		public void reduce(DoubleWritable key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			int numOfStocks = 0;
			List<String> stockNameList = new ArrayList<String>();
			try {
				/* Get num of stocks */
				String numOfStocksStr = context.getConfiguration().get(
						"numOfStocks");
				if (Main.isNumeric(numOfStocksStr))
					numOfStocks = Integer.parseInt(numOfStocksStr);

				/* There may be a chance that multiple 
				 * stocks have the same volatility value */
				for (Text stockName: iterable)
					stockNameList.add(stockName.toString());
				String counterString = new Integer(counter).toString();
				
				for (String stockName: stockNameList) {
					/* If it's in the top 10 or bottom 10, write it */
					if (counter <= 10 || counter > numOfStocks-10) {
						byte[] rowId = Bytes.toBytes(counterString);
						Put put = new Put(rowId);
						put.add(Bytes.toBytes("stock"), Bytes.toBytes("name"), stockName.getBytes());
						
						DecimalFormat df = new DecimalFormat("0.00000000");
						df.setMaximumFractionDigits(8);
						String vol = df.format(key.get());
						
						put.add(Bytes.toBytes("price"), Bytes.toBytes("volatility"), vol.getBytes());
						context.write(new ImmutableBytesWritable(rowId), put);
					}
					counter++;
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}