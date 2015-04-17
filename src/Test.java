import java.text.DecimalFormat;


public class Test {

	public static void main(String[] args) {
		double d = 0.000004;
		
		DecimalFormat df = new DecimalFormat("0.0");
		df.setMaximumFractionDigits(10);
		String s = df.format(d);
		d = Double.parseDouble(s);
		
		
		System.out.println();
	}

}
