
import static org.junit.Assert.*;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DefaultTuple;
import org.junit.Test;

public class IsValidDateTest {
	private static final EvalFunc<Boolean> IsValidDate_ = new IsValidDate();

	@Test
	public void nullDate() throws IOException {
		String inputDate = null;
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	public void ZeroDate() throws IOException {
		String inputDate = "20000000000000";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	public void nullTuple() throws IOException {
		//String inputDate = null;
		//DefaultTuple input = new DefaultTuple();
	    //input.append(inputDate);
		assertFalse(IsValidDate_.exec(null));
	}
	
	@Test
	public void emptyDate() throws IOException {
		String inputDate = "";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}

	@Test
	// short date -> yyyymmddhhmm
	public void shortDateMinute_UTC() throws IOException {
		String inputDate = "201801010101";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	// short date -> yyyymmddhh
	public void shortDateHH_UTC() throws IOException {
		String inputDate = "2018010101";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	// All fields valid except future year -> 2024
	public void AllValidExFutureYear_UTC() throws IOException {
		String inputDate = "20240101010101";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	// All fields valid but hhmmss =00 -> 20170101000000
	public void AllValid_UTC() throws IOException {
		String inputDate = "20170101000000";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertTrue(IsValidDate_.exec(input));
	}
	
	@Test
	// All fields valid but year =0000 -> 00000101000000
	public void AllValidYearInvalid_UTC() throws IOException {
		String inputDate = "00000101000000";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	
	
	@Test
	//yyyy mm dd hh mm ss
	//2018 09 26 00 00 0
	public void valiDateFutureFromActualData_UTC() throws IOException {
		String inputDate = "20420627010340";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	//yyyy mm dd hh mm ss
	//2018 09 26 00 00 0
	public void valiDate_UTC() throws IOException {
		String inputDate = "20180926000000";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertTrue(IsValidDate_.exec(input));
	}
	
	@Test
	public void valiDate_nowUTC() throws IOException {
		String inputDate = "20180927061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertTrue(IsValidDate_.exec(input));
	}
	
	@Test
	//Month =00
	public void zeroMonth_UTC() throws IOException {
		String inputDate = "20170027061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	//Day =00
	public void zeroValidDay_UTC() throws IOException {
		String inputDate = "20170100061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	//Month =13
	public void inValidMonth_UTC() throws IOException {
		String inputDate = "20171327061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	//Day =32 for january
	public void inValidDay_UTC() throws IOException {
		String inputDate = "20170132061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	//Day =29 for feb 2018
	public void inValidDayFeb_UTC() throws IOException {
		String inputDate = "20170229061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	@Test
	//Day =28 for feb 2018
	public void ValidDayFeb_UTC() throws IOException {
		String inputDate = "20170228061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertTrue(IsValidDate_.exec(input));
	}
	
	@Test
	//Day =29 for feb 2016
	public void ValidDayLastFeb_UTC() throws IOException {
		String inputDate = "20160229061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertTrue(IsValidDate_.exec(input));
	}
	
	@Test
	//Day =30 for feb 2016
	public void inValidDayLastFeb_UTC() throws IOException {
		String inputDate = "20160230061308";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
	
	//At the time of test, UTC value = 20180927064349
	@Test
	public void invaliDate_futureUTC() throws IOException {
		String inputDate = "20180927065049";
		DefaultTuple input = new DefaultTuple();
	    input.append(inputDate);
		assertFalse(IsValidDate_.exec(input));
	}
}
