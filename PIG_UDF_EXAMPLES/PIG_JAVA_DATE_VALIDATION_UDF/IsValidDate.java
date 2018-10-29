
//javac -cp `hadoop classpath`:pig-0.16.0.2.5.5.0-157-core-h2.jar IsValidDate.java

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class IsValidDate extends FilterFunc {
	DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
	Date ParsedDateUTC;
	String nowDateUTC;
	Date nowDateUTC_D;

	@Override
	public Boolean exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) 
			return false;
		if(input.get(0) == null)
			return false;

		String inputDate = (String) input.get(0);
		//<yyyymmddhhmmss>
		if(inputDate != null && inputDate.trim().length() == 0) 
			return false;

		dateFormat.setLenient(false) ;
		dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

		try { // Validate the Valid date format and value using java standard API
			ParsedDateUTC = dateFormat.parse(inputDate);

		}catch (ParseException e) { 
			System.out.println (e.getMessage()) ;
			return false;}

		//Check Date is not from Future, if yes return false.
		//nowDateUTC = LocalDateTime.now(); 
		Date nowDateMiliis = new Date();
		nowDateUTC = dateFormat.format(nowDateMiliis);


		try {
			nowDateUTC_D = dateFormat.parse(nowDateUTC);
		} catch (ParseException e) {
			System.out.println (e.getMessage()) ;
			//return false;
		}

		if(nowDateUTC_D.before(ParsedDateUTC))
			return false;
		else
			return true;
	}

}
