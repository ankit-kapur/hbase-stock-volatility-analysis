import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class Job4 {

	static int counter=1;
	public static class MyMapper extends TableMapper<Text, Text> {
		public void map(ImmutableBytesWritable row, Result result,
				Context context) throws IOException, InterruptedException {
			String stockName = new String(result.getValue("stock".getBytes(),
					"name".getBytes()));
			String volatility = new String(result.getValue("price".getBytes(),
					"volatility".getBytes()));

			String key = volatility;
			String value = stockName;

			context.write(new Text(key), new Text(value));
		}
	}

	/* Reducer */
	public static class MyTableReducer extends
			TableReducer<Text, Text, ImmutableBytesWritable> {

		public void reduce(Text key, Iterable<Text> iterable, Context context)
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

				System.out.println(new Integer(counter).toString() + " )))) " + stockNameList + " )))) " + key.toString());
				
				for (String stockName: stockNameList) {
					/* If it's in the top 10 or bottom 10, write it */
					if (counter <= 10 || counter > numOfStocks-10) {
						byte[] rowId = Bytes.toBytes(new Integer(counter).toString());
						Put put = new Put(rowId);
						put.add(Bytes.toBytes("name"), Bytes.toBytes("stock"), stockName.getBytes());
						put.add(Bytes.toBytes("price"), Bytes.toBytes("volatility"), key.toString().getBytes());
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