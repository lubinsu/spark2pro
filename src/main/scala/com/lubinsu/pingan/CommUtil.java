package com.lubinsu.pingan;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.security.cert.CertificateException;
import java.text.SimpleDateFormat;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;


public class CommUtil {
	private static final Logger logger = Logger.getLogger(CommUtil.class);

	public static int diffDays(String format, String date1_str, long date2_long) {
		int days = 0;
		try {
			days = (int) ((date2_long - new SimpleDateFormat(format).parse(date1_str).getTime()) / (1000 * 3600 * 24));
		} catch (Exception e) {
			logger.error("diffDays-error", e);
		}
		return days;
	}

	/**
	 * 通过时间秒毫秒数判断两个时间的间隔
	 * 
	 * @return
	 */
	public static int differentDaysByMillisecond(String format, String date1_str, long date2_long) {
		int days = 0;
		try {
			days = (int) ((new SimpleDateFormat(format).parse(date1_str).getTime() - date2_long) / (1000 * 3600 * 24));
		} catch (Exception e) {
			logger.error("differentDaysByMillisecond出现异常：", e);
		}
		return days;
	}

	public static String httpPost(String url, String params) {
		String respContent = "{}";
		HttpClient client = new DefaultHttpClient();
		client.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 120 * 1000);
		client.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 120 * 1000);
		try {
			HttpPost httpPost = new HttpPost(url);
			StringEntity entity = new StringEntity(params, "utf-8");// 解决中文乱码问题
			entity.setContentEncoding("UTF-8");
			entity.setContentType("application/json");
			httpPost.setEntity(entity);
			HttpResponse resp = client.execute(httpPost);
			System.out.println(resp.getStatusLine().getStatusCode());
			if (resp.getStatusLine().getStatusCode() == 200) {
				
			}
				HttpEntity content = resp.getEntity();
				respContent = EntityUtils.toString(content, "UTF-8");
			
			
		} catch (Exception e) {
			logger.error("httpPost-error " + url, e);
		} finally {
			if (null != client) {
				try {
					client.getConnectionManager().shutdown();
				} catch (Exception e) {
					logger.error("client.close()-error " + url, e);
				}
			}
		}
		return respContent;
	}
	
	

	/**
	 * 向指定URL发送GET方法的请求
	 * 
	 * @param url
	 *            发送请求的URL
	 * @param param
	 *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	 * @return URL 所代表远程资源的响应结果
	 */
	public static String sendGet(String url, String param) {
		String result = "";
		BufferedReader in = null;
		try {
			String urlNameString = url;
			if (!StringUtils.isEmpty(param)) {
				urlNameString = url + "?" + param;
			}
			URL realUrl = new URL(urlNameString);
			// 打开和URL之间的连接
			URLConnection connection = realUrl.openConnection();
			// 设置通用的请求属性
			connection.setRequestProperty("accept", "*/*");
			connection.setRequestProperty("Accept-Language", "zh-cn");
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 请求超时时间
			connection.setConnectTimeout(30 * 1000);
			// 连接超时时间
			connection.setReadTimeout(30 * 1000);
			// 建立实际的连接
			connection.connect();
			// 获取所有响应头字段
			in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			logger.error("sendGet()-error " + url, e);
		}
		// 使用finally块来关闭输入流
		finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				logger.error("in.close()-error " + url, e2);
			}
		}
		return result;
	}

	
	/**
	 * 向指定URL发送GET方法的请求
	 * 
	 * @param url
	 *            发送请求的URL
	 * @param param
	 *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	 * @return URL 所代表远程资源的响应结果
	 */
	public static String sendHttpsPost(String url, String param) {
		String result = "";
		BufferedReader in = null;
		try {
			// 创建SSLContext对象，并使用我们指定的信任管理器初始化
			// TrustManager[] tm = { new MyX509TrustManager() };
			// SSLContext sslContext = SSLContext.getInstance("SSL", "SunJSSE");
			// sslContext.init(null, tm, new java.security.SecureRandom());
			// SSLContext sslContext =
			// SSLContext.getDefault();//.getInstance("SSLv3");
			SSLContext sslContext = SSLContext.getInstance("TLS");
			// 实现一个X509TrustManager接口，用于绕过验证，不用修改里面的方法
	        X509TrustManager trustManager = new X509TrustManager() {  
				@Override
				public void checkClientTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate, String paramString) {
				}
				@Override
				public void checkServerTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate, String paramString) {
				}
				@Override
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}
	        };  
	        sslContext.init(null, new TrustManager[] { trustManager }, null);
	        
	        // 从SSLContext对象中得到SSLSocketFactory对象
	        SSLSocketFactory ssf = sslContext.getSocketFactory();
			String urlNameString = url ;
			URL realUrl = new URL(urlNameString);
			// 创建HttpsURLConnection对象，并设置其SSLSocketFactory对象
	        HttpsURLConnection connection = (HttpsURLConnection) realUrl.openConnection();
	        connection.setRequestMethod("POST");
	        connection.setDoOutput(true);
	        connection.setDoInput(true);
	        connection.setSSLSocketFactory(ssf);
			// 打开和URL之间的连接
			// URLConnection connection = realUrl.openConnection();
			// 设置通用的请求属性
			connection.setRequestProperty("accept", "*/*");
			connection.setRequestProperty("Accept-Language", "zh-cn");
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 请求超时时间
			connection.setConnectTimeout(30 * 1000);
			// 连接超时时间
			connection.setReadTimeout(30 * 1000);
			// 建立实际的连接
			connection.connect();
			
			PrintWriter printWriter = new PrintWriter(connection.getOutputStream());
            // 发送请求参数
            printWriter.write(param);//post的参数 xx=xx&yy=yy
            // flush输出流的缓冲
            printWriter.flush();
	            
			// 获取所有响应头字段
			in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			logger.error("sendGet()-error " + url, e);
		}
		// 使用finally块来关闭输入流
		finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				logger.error("in.close()-error " + url, e2);
			}
		}
		return result;
	}
	
	/**
	 * 向指定URL发送GET方法的请求
	 * 
	 * @param url
	 *            发送请求的URL
	 * @param param
	 *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式。
	 * @return URL 所代表远程资源的响应结果
	 */
	public static String sendHttpsGet(String url, String param) {
		String result = "";
		BufferedReader in = null;
		try {
			// 创建SSLContext对象，并使用我们指定的信任管理器初始化
			// TrustManager[] tm = { new MyX509TrustManager() };
			// SSLContext sslContext = SSLContext.getInstance("SSL", "SunJSSE");
			// sslContext.init(null, tm, new java.security.SecureRandom());
			// SSLContext sslContext =
			// SSLContext.getDefault();//.getInstance("SSLv3");
			SSLContext sslContext = SSLContext.getInstance("TLS");
			// 实现一个X509TrustManager接口，用于绕过验证，不用修改里面的方法
	        X509TrustManager trustManager = new X509TrustManager() {  
				@Override
				public void checkClientTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate, String paramString) throws CertificateException {
				}
				@Override
				public void checkServerTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate, String paramString) throws CertificateException {
				}
				@Override
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}
	        };  
	        sslContext.init(null, new TrustManager[] { trustManager }, null);
	        
	        // 从SSLContext对象中得到SSLSocketFactory对象
	        SSLSocketFactory ssf = sslContext.getSocketFactory();
			String urlNameString = url + "?" + param;
			URL realUrl = new URL(urlNameString);
			// 创建HttpsURLConnection对象，并设置其SSLSocketFactory对象
	        HttpsURLConnection connection = (HttpsURLConnection) realUrl.openConnection();
	        connection.setSSLSocketFactory(ssf);
			// 打开和URL之间的连接
			// URLConnection connection = realUrl.openConnection();
			// 设置通用的请求属性
			connection.setRequestProperty("accept", "*/*");
			connection.setRequestProperty("Accept-Language", "zh-cn");
			connection.setRequestProperty("connection", "Keep-Alive");
			connection.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
			// 请求超时时间
			connection.setConnectTimeout(30 * 1000);
			// 连接超时时间
			connection.setReadTimeout(30 * 1000);
			// 建立实际的连接
			connection.connect();
			// 获取所有响应头字段
			in = new BufferedReader(new InputStreamReader(connection.getInputStream(), "utf-8"));
			String line;
			while ((line = in.readLine()) != null) {
				result += line;
			}
		} catch (Exception e) {
			logger.error("sendGet()-error " + url, e);
			result = "";
		}
		// 使用finally块来关闭输入流
		finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (Exception e2) {
				logger.error("in.close()-error " + url, e2);
			}
		}
		return result;
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
