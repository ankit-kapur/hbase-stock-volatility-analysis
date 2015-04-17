import java.text.DecimalFormat;


public class Test {

	public static void main(String[] args) {
		double d = 123/3443423234.0;
		
		DecimalFormat df = new DecimalFormat("0.0");
		df.setMaximumFractionDigits(10);
		String s = df.format(d + 22.0);
		
		System.out.println(s);
	}

}
