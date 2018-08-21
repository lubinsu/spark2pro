package com.lubinsu.pingan;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

public class PingAn3 {
	public static void main(String[] args) throws Exception {

		// kDsMeVjo3guMp3kjLPkk
		// https://finance-api.fengkongcloud.com/v3/finance/overdue
		// System.out.println(date2UnixTime("2017-07-22 09:30:46", "yyyy-MM-dd HH:mm:ss"));

		// {"accessKey":"kDsMeVjo3guMp3kjLPkk","data":{"phone":"15025980461","name":"王伟兵","prcid":"620502198912165337"}}

		PrintWriter printWriter_1 = new PrintWriter("C:\\Users\\Administrator\\Desktop\\tmpDir\\pingan\\out\\pingan_04_23.txt");
		
		CsvReader csvReader = new CsvReader("C:\\Users\\Administrator\\Desktop\\tmpDir\\pingan\\time_test_cuishou0630.txt", '\t', Charset.forName("UTF-8"));
		
		String vkey = "120170919001";
		String t = "dun_number_mark";
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("t=" + t + "&");
		stringBuilder.append("vkey=" + vkey + "&");
		
		StringBuilder ss = new StringBuilder();
		int count = 0;

		String valStr = "";
		while (csvReader.readRecord()) {
			valStr = csvReader.getRawRecord();
			valStr = valStr.replaceAll("\"", "");
			String[] vals = valStr.split("\t", -1);
			String number = "";
			if (!StringUtils.isEmpty(vals[0])) {
				number = vals[0].trim();
			}
			if (count==99) {
				
				boolean flag = false;
				String result_1  ="";
				
				while (false==flag) {
					result_1 = CommUtil.sendHttpsGet("https://grayscale.pacra.cn/service", stringBuilder.toString()+"number="+ss.toString());
					if (!StringUtils.isEmpty(result_1)) {
						//System.out.println(result_1);
						printWriter_1.write(ss.toString() + "\t" + result_1 + "\n");
						printWriter_1.flush();
						ss = new StringBuilder();
						count = 0;
						flag = true;
					}else {
						System.out.println("进入休眠...");
						Thread.sleep(30*1000);
					}
				}
			}else {
				number = number.replaceAll("\\+86|\\#|\\+|\\*", "");
				if (isNumeric(number)) {
					count++;
					ss.append(number+",");
				}
			}

		}
		printWriter_1.close();
	}

	private static boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

	public static String date2UnixTime(String dateStr, String format) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			return String.valueOf(sdf.parse(dateStr).getTime() / 1000);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

}
