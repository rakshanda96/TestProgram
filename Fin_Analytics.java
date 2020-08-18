import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.itextpdf.text.BaseColor;
import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Element;
import com.itextpdf.text.Font;
import com.itextpdf.text.Phrase;
import com.itextpdf.text.Font.FontFamily;
import com.itextpdf.text.pdf.ColumnText;
import com.itextpdf.text.pdf.PdfCopy;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.PdfSmartCopy;
import com.itextpdf.text.pdf.PdfStamper;
import com.crystaldecisions.sdk.exception.SDKException;
import com.crystaldecisions.sdk.framework.IEnterpriseSession;
import com.crystaldecisions.sdk.occa.infostore.IInfoStore;

public class Fin_Analytics implements com.crystaldecisions.sdk.plugin.desktop.program.IProgramBase{
	private static final Logger logger = Logger.getLogger( Fin_Analytics.class); 
	public static String finalPages;
	public final static String mainFolder=File.separator+"BO_FTP";
	

	public void run(IEnterpriseSession arg0, IInfoStore arg1, String[] args) throws SDKException{
		// TODO Auto-generated method stub
			if(args.length>0 && args[0].length()>0){
					 finalPages=args[0];
					 }else{
					 finalPages=null;
				 }

		BasicConfigurator.configure(); //enough for configuring log4j
		Logger.getRootLogger().setLevel(Level.INFO); //changing log level
		if(new File(File.separator+"BO_FTP").exists() && new File(File.separator+"BO_FTP").isDirectory()){
			try{
				if(new File(mainFolder+File.separator+"Fin_Analytics").exists() && new File(mainFolder+File.separator+"Fin_Analytics").listFiles().length>0){
					{
						try {
							insert_PDF();
							logger.info("files are uploaded successfully into MIS DB");
						}catch(Exception  e){
							logger.error(e.getMessage());

						}
					}
				}else {
					logger.error(" No Files Found");
				}
				
			}catch(Exception e) {
					logger.error(e.getMessage());
					
			}finally {
	
					}
		}else{
				logger.error("There is no Mount Point- BO_FTP");
			}
	}
	public static void insert_PDF() throws FileNotFoundException{
			PreparedStatement pstmtInsert = null;
			PreparedStatement pstmtUpdate = null;
			Connection con = null;
			FileInputStream fis=null;
			String password= null;
			InputStream input=new FileInputStream(new File(mainFolder+File.separator+"DbConn.properties"));
			Properties prop=new Properties();
			password= new String();
			File finalFolder = new File(mainFolder+File.separator+"Fin_Analytics");
			File[] files = finalFolder.listFiles();
			try {
				 prop.load(input);
				 String driverName=prop.getProperty("DriverName").toString().trim();
				 String url =prop.getProperty("url").toString().trim();
				 String user =prop.getProperty("UserName").toString().trim();
				 for(String a:new StringBuffer(prop.getProperty("Pwd").toString().trim()).reverse().toString().split(",")){
					if(a.length()>0){
							password=password+Character.toString((char)Integer.parseInt(a));
					}
				 }
				 if(password.length()>0 && url.length()>0 && user.length()>0 && driverName.length()>0){
					 	Class.forName(driverName);
					 	con = DriverManager.getConnection(url,user,password);
					 	con.setAutoCommit(false);
					 	String Insert_PDF= "INSERT INTO EA_HR.OPS_REVIEW_REPORT (REPORT_FILE_NAME, REPORT_MEETING_DATE, REPORT_UNIT_CODE, REPORT_DOC, REPORT_STATUS, CREATED_BY, CREATED_DATE, LAST_UPDATED_BY, LAST_UPDATED_DATE,REPORT_SEQ_ID,TENANT_IDENTIFIER)" 
										+"VALUES(?,to_timestamp(?,'yyyy-mm-dd-HH24-mi-ss'),?,?,?,?,sysdate,?,sysdate,(select nvl(max(REPORT_SEQ_ID),0)+1 as ID from EA_HR.OPS_REVIEW_REPORT)"+",?)";

					 	String Update_PDF="UPDATE EA_HR.OPS_REVIEW_REPORT set  REPORT_STATUS='OLD' where REPORT_STATUS='NEW' AND REPORT_UNIT_CODE=? AND TENANT_IDENTIFIER='FINANCIAL_ANALY'";
					 	if (files != null){
					 		for(File eachFile: files){
					 			try {
					 				String fileName = eachFile.getName();
								String date=new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(Calendar.getInstance().getTime()).toString();
								logger.info("fileName =  "+fileName);
								String qulifiedFileName = fileName.substring(0, fileName.lastIndexOf('.')); 					    	
						    	logger.info("QulifiedFileName = "+qulifiedFileName);
						    	logger.info("DateTime = to_timestamp('"+date+"','yyyy-mm-dd-HH24-mi-ss')");
						    	logger.info("iouCode = "+qulifiedFileName);
						    	logger.info("Status = new");
						    	fis = new FileInputStream(eachFile);
						    	pstmtInsert = con.prepareStatement(Insert_PDF);					
						    	pstmtInsert.setString(1, qulifiedFileName); // REPORT_FILE_NAME
						    	pstmtInsert.setString(2, date);//REPORT_MEETING_DATE
						    	pstmtInsert.setString(3, qulifiedFileName);//REPORT_UNIT_CODE					
						    	pstmtInsert.setBinaryStream(4, fis, (int) eachFile.length());//REPORT_DOC
						    	pstmtInsert.setString(5, "NEW");//REPORT_STATUS
						    	pstmtInsert.setString(6, "1111111");//CREATED_BY
						    	pstmtInsert.setString(7, "1111111");//LAST_UPDATED_BY
						    	pstmtInsert.setString(8,"FINANCIAL_ANALY");//TENTANT_IDENTIFIER
						    	pstmtUpdate=con.prepareStatement(Update_PDF);
								pstmtUpdate.setString(1, qulifiedFileName);
								pstmtUpdate.executeUpdate();
								int rowsInserted = pstmtInsert.executeUpdate();
								con.commit();
								if (rowsInserted > 0) {
									logger.info(fileName + " uploaded successfully!");
								}else{
									logger.info("1 file skipped");
									logger.info("Skipped FileName="+eachFile.getName());
									continue;
								}
					 		}catch(Exception e){
			        		logger.error(e.getMessage());
				    		logger.error("1 file skipped");
				    		logger.error("Skipped FileName="+eachFile.getName());
				    		continue;
			        	}
					}
			     }
			}
			}catch(Exception e){
						logger.error(e.getMessage());
					}finally{
			        		if(pstmtInsert!=null){
			        			try {
			        				pstmtInsert.close();
			        				} catch (Exception e2){
			        					logger.error(e2.getMessage());

						}
			    	}
					if(pstmtUpdate!=null){
			    		try {
			    			pstmtUpdate.close();
						} catch (Exception e2){
							logger.error(e2.getMessage());

						}
			    	}
			    	if(con!=null){
			    		try {
							con.close();
						} catch (Exception e2) {
							logger.error(e2.getMessage());

						}
			    	}
			    	password=null;
				}

	}

	
}