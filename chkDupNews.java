import QueryAPI530.Search;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.util.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import kr.co.wisenut.bridge3.config.*;
import kr.co.wisenut.bridge3.*;
import kr.co.wisenut.bridge3.config.catalogInfo.Mapping;
import kr.co.wisenut.bridge3.config.datasource.*;
import kr.co.wisenut.bridge3.config.datasource.DataSource;
import kr.co.wisenut.common.util.EncryptUtil;
import org.apache.log4j.Logger;


public class chkDupNews {

   static ArrayList<String> userKeywordList=null;
  
  //환경설정파일 위치
  static String properityFile="/home/newsfilter/sf-1/config/chkDupNews.properties";  
  
  //검색엔진 관련정보 설정
  static String COLLECTION="newsdupcheck";
  static String SERVER_IP="127.0.0.1";
  static int SERVER_PORT=7000;
  static int SERVER_TIMEOUT=5*1000;  
  
  //검색엔진에 정의된 필드목록
  static String DOCUMENT_FIELD="INPUTDAT,INFODIVI,CLASCODE,NEWSNUMB,INFCLASS,INPUTPST,INPUTPER,TITLE,DATOFFER,ENTPCODE,CATECODE,GRUPCODE,ORGCLCOD,APPEFILE,SIDETRAN,INQUICNT,FORESYMB,NEWSUAMT,NEWSDIVI,KEYWORD";
  // DB에 정의된 필드목록(검색엔진 필드목록과 순서가 동일해야함)
  static String DB_FIELD="INPUTDAT,INFODIVI,CLASCODE,NEWSNUMB,INFCLASS,INPUTPST,INPUTPER,NEWTITLE,DATOFFER,ENTPCODE,CATECODE,GRUPCODE,ORGCLCOD,APPEFILE,SIDETRAN,INQUICNT,FORESYMB,NEWSUAMT,NEWSDIVI,PUSH_KEYWORD";

  //뉴스기사를 읽어들일 디렉토리 위치  
  static String SCDBackupPath="/home/newsfilter/sf-1/collection/newsdupcheck/scd/backup/";
  //처리완료 후 저장할 백업디렉토리 위치
  static String DupCheckBackupPath="/home/newsfilter/sf-1/collection/newsdupcheck/scd/dupbackup/";
  //오류발생시 저장할 디렉토리 위치
  static String DupCheckErrorPath="/home/newsfilter/sf-1/collection/newsdupcheck/scd/duperror/";    

  //DB관련 설정
  // DB접속 정보가 저장된 설정파일 지정
  static String dataSourcePath="/home/newsfilter/sf-1/config/datasource.xml";
  static String dsn="sknews_db";      
  
  //중복실행 방지를 위한 타켓파일
  static String targetFilePath ="/home/newsfilter/sf-1/collection/target/chkDupNews.done";
  
  private Config m_config = new Config();
  protected static Logger logger = Logger.getLogger(chkDupNews.class);
  
  
public static void main(String[] args) throws Exception
{

	  Connection conn=null;


try {
      //실행 시작
      long time = System.currentTimeMillis();
      SimpleDateFormat daytime= new SimpleDateFormat("yyyy/MM/dd kk:mm:ss");
      String startDateTime = daytime.format(new java.util.Date(time));
      logger.info("================================================");
      logger.info("Run Date/Time : "+startDateTime);
      logger.info("================================================");  

      
      //설정파일을 읽어 들임  
      readProperty();
    
      //실행중인 아니면
      if (!isRunning()){
    
      // 색인완료(증분색인)된 SCD 파일을 목록중 1개만 가져옴
      String SCDFile=getFileList(SCDBackupPath);
    
      //SCD파일이 있으면
      if (!SCDFile.equals("")){
        logger.info("SCD File:"+SCDBackupPath+SCDFile);
        ArrayList<Hashtable<String,String>> uniqueResult = new ArrayList<Hashtable<String,String>>();
		conn = createDBConnection();
        uniqueResult = checkUniqueNewsData(DOCUMENT_FIELD,SCDReadDOCID(SCDBackupPath+SCDFile),conn);
        
        logger.info("*Unique News Count:"+uniqueResult.size());
        
        Hashtable ht = InsertDBNews(DOCUMENT_FIELD,DB_FIELD,uniqueResult,conn);
    
        
        //정상적으로 DB에 입력되었으면 DupCheckBackupPath 이동 
        if (ht.get("runFlag").equals("true")){
            moveSCDToTarget(SCDBackupPath,DupCheckBackupPath,SCDFile);
       logger.info("================================================");              
             logger.info("*DB Insert successCount:"+ht.get("successCount"));
             logger.info("*DB Insert failCount:"+ht.get("failCount"));      
             logger.info("DB Insert Job Complete!");          
            
        }else{ // DB입력시 오류가 발생했으면DupCheckErrorPath 이동 
            logger.info("DB Insert Error!");    
            moveSCDToTarget(SCDBackupPath,DupCheckErrorPath,SCDFile);    
        }
      }else{
       logger.info("These is no Dynamic SCD File!");
      }
      
    
      
      }else{
         logger.info("Running chkDupNews (or Delete file for restart - "+targetFilePath+")");
      }
  
  }catch(Exception e){
    logger.error("main() Error:"+e.getMessage());
	conn.close();
  }finally{
	if (conn!=null) conn.close();

    runFinish();
  }


} // End of main()  



//DB에서 사용자가 등록한 키워드 목록을 가져온다.
public static ArrayList<String> getUserKeywordList(Connection conn) throws Exception
{

ArrayList<String> DBUserKeywordList = new ArrayList<String>();



String sql="SELECT DISTINCT(KEYWORD) FROM NEWSKEYWORD ";


	  logger.debug("getUserKeywordList SQL :"+sql);

  PreparedStatement pstmt = null;
  ResultSet rs = null;

  try{
      pstmt=conn.prepareStatement(sql);
	  rs=pstmt.executeQuery();
	  while(rs.next()){
		if (rs.getString("KEYWORD").length() <=1) continue; //글자 개수가 1글자 또는 0이면 제외
		DBUserKeywordList.add(rs.getString("KEYWORD"));
	  }
   }catch(Exception e){
        logger.info("getUserKeywordList SQL Error : "+e.getMessage());
  }finally{
	  logger.debug(">>>>>>>>>>>> getUserKeywordList Finally");
      if (rs != null) rs.close();
      if (pstmt != null) pstmt.close();
  }


return DBUserKeywordList;

}


// 사용자가 등록한 키워드와 뉴스기사의 키워드와 비교 함수 - 매칭된 사용자 등록키워드 목록 리턴
public static String getMatchUserKeyword(String keyword,Connection conn) throws Exception
{

String matchKeywordList = "";

//최초 한번만 DB에서 사용자가 등록한 키워드를 읽음
if (userKeywordList == null) userKeywordList=getUserKeywordList(conn);



boolean initFlag=true;
for (int i=0 ; i < userKeywordList.size();i++ )
{
	if (keyword.indexOf(userKeywordList.get(i))!=-1)
	{
		if (initFlag) {
			matchKeywordList=userKeywordList.get(i);
			initFlag=false;
		}else{
			matchKeywordList+="|"+userKeywordList.get(i);
		}
	}		

}

logger.debug("matchKeywordList:"+matchKeywordList);
return matchKeywordList;
}
    

//환경파일 정보를 읽어 DB 커넥션 생성하기
public static Connection createDBConnection() throws Exception{

  Connection conn = null;
    //datasource.xml에서 DB접속정보를 읽는다.
    HashMap m_dsMap;
    Config m_config = new Config();
    m_config.setDataSource(new GetDataSource(dataSourcePath).getDataSource());
    m_dsMap=m_config.getDataSource();

    String SERVERNAME =((DataSource)m_dsMap.get(dsn)).getServerName();      
    String PORT=((DataSource)m_dsMap.get(dsn)).getPort();      
    String SID=((DataSource)m_dsMap.get(dsn)).getSid();      
    String CLASSNAME =((DataSource)m_dsMap.get(dsn)).getClassname();              

    String USER=((DataSource)m_dsMap.get(dsn)).getUser();      
    String PWD=((DataSource)m_dsMap.get(dsn)).getPwd();  

    //DB ID/PW 복호화 처리
    if(EncryptUtil.isHexa(USER)) {
      
        if(USER.length() > 4 && PWD.length() >4){
            USER = USER.substring(4, USER.length());
            PWD = PWD.substring(4, PWD.length());
        }
        USER =  EncryptUtil.decryptString(USER) ;
        PWD =  EncryptUtil.decryptString(PWD) ;
    } else if(System.getProperty("datasource.encrypt") != null) {
      
    	String encrypt = System.getProperty("datasource.encrypt");
    	
    	if(encrypt.equalsIgnoreCase("aes")) {
    		USER =  EncryptUtil.decryptStringAES(USER) ;
            PWD =  EncryptUtil.decryptStringAES(PWD) ;
        	}
        	
        }    
  
  String dbURL="jdbc:oracle:thin:@"+SERVERNAME+":"+PORT+":"+SID;
  String dbClassName=CLASSNAME;
  logger.info("DB JDBC URL : "+dbURL);


    Class.forName(dbClassName);
    conn = DriverManager.getConnection(dbURL,USER,PWD);
    if (conn == null){
      logger.info("DB Connection Fail!");
	}

return conn;
}




//중복안된 뉴스기사 DB에 Insert(datasource.xml에서 DB접속정보 추출)
public static Hashtable<String,String> InsertDBNews(String DOCUMENT_FIELD ,String DB_FIELD,ArrayList<Hashtable<String,String>> uniqueResult,Connection conn) throws Exception
{

  String[] ARR_DOCUMENT_FIELD = DOCUMENT_FIELD.split(",");
  String[] ARR_DB_FIELD = DB_FIELD.split(",");
  String sql="";

  sql = "insert into NEWSFILTER("+DB_FIELD+",DUPYN,NEWS_SEQ)";
  sql+="values(";
  for (String question :ARR_DB_FIELD){
  sql+="?,";
  }
  sql+="'N',"; //DUPYN
  sql+="NEWSFILTER_SEQ01.NEXTVAL"; //NEWS_SEQ
  sql+=")"; 

  logger.debug("Prepared sql:"+sql);


  int failCount=0;
  int successCount=0;  
  Hashtable<String,String> ht = new Hashtable<String,String>();
  
  boolean runFlag=false;


  PreparedStatement pstmt = null;

  try{
      conn.setAutoCommit(true);    
 
      pstmt=conn.prepareStatement(sql);
      for (Hashtable<String,String> setVal : uniqueResult){
        try{
			   for (int i=0 ; i <setVal.size();i++){
				    pstmt.setString(i+1,setVal.get(ARR_DOCUMENT_FIELD[i]));
			   }
          int cnt = pstmt.executeUpdate();
          successCount++;
        }catch(Exception e){
        logger.info("DB Insert SQL Error : "+e.getMessage());      
        logger.info("Error SQL : "+sql);
        failCount++;
        }
      }
          runFlag=true;

  }catch(Exception e){
    logger.error("InsertDBNews DB Error:"+e.getMessage());
    runFlag=false;    
  }finally{
      if (pstmt != null) pstmt.close();
      if (conn != null) conn.close();    
  }

   ht.put("runFlag",String.valueOf(runFlag));
   ht.put("successCount",String.valueOf(successCount));
   ht.put("failCount",String.valueOf(failCount));      
   
  
  return ht;


}

  

//아래의 뉴스기사 중복체크 함수를 사용시 반드시 
// <DupDetect 내에 runafter="prefix" 설정되어 있어야 함
// 뉴스기사가 중복되었는지 체크 후 중복이 안된 뉴스만 데이터 전달
public static ArrayList<Hashtable<String,String>> checkUniqueNewsData(String DOCUMENT_FIELD,ArrayList<String> docidlist,Connection conn) throws Exception
{

    ArrayList<Hashtable<String,String>> alResult = new ArrayList<Hashtable<String,String>>();
    Hashtable<String,String> result = null;

    int ret = 0;
    int dupTotalCount = 0;
    String query="";
    int QUERY_LOG=0;    
    int EXTEND_OR=0;
    int PAGE_START=0;
    int RESULT_COUNT=1000; //최대 10000 건까지만 SCD데이터를 중복체크 후 DB입력 할수있음
    String SORT_FIELD ="DATE/DESC";
    String SEARCH_FIELD="TITLE,CONTENT";
    String[] ARR_DOCUMENT_FIELD = DOCUMENT_FIELD.split(",");
    QueryAPI530.Search search = new QueryAPI530.Search();

    ret=search.w3SetCodePage("UTF-8");
    ret=search.w3SetQueryLog(QUERY_LOG);
    ret=search.w3SetCommonQuery(query,EXTEND_OR);
    
    ret=search.w3AddCollection(COLLECTION);
    ret=search.w3SetPageInfo(COLLECTION,PAGE_START,RESULT_COUNT);
    ret=search.w3SetSortField(COLLECTION,SORT_FIELD);     
    ret=search.w3SetSearchField(COLLECTION,SEARCH_FIELD);
    ret=search.w3SetDocumentField(COLLECTION,DOCUMENT_FIELD);
    
    String filterQuery="";
    for (String docid : docidlist){    
        if (filterQuery.equals("")) filterQuery="<DOCID:match:"+docid+">";
        else filterQuery+="|<DOCID:match:"+docid+">";
    }
    ret=search.w3SetFilterQuery(COLLECTION,filterQuery);        

    ret=search.w3SetDuplicateDetection(COLLECTION);                       
    
    ret = search.w3ConnectServer(SERVER_IP,SERVER_PORT,SERVER_TIMEOUT);    
    ret = search.w3ReceiveSearchQueryResult(3);    
    
    if (search.w3GetError()!=0){
      logger.error(search.w3GetErrorInfo());
      runFinish();
    }
     
    int totalCount=search.w3GetResultTotalCount(COLLECTION);
    logger.info("*News totalCount:"+totalCount);
    int count= search.w3GetResultCount(COLLECTION);
    int dupCount=0;
    
    if (totalCount>0)  //뉴스데이터가 색인이 완료되어 검색기에 반영 되었으면
    {    
      for (int i=0 ; i<count ; i++)
      {
        dupCount=search.w3GetDuplicateDocumentCount(COLLECTION,i);
		String ISAD=search.w3GetField(COLLECTION,"ISAD",i); //광고성 기사 유무



        logger.debug("DOCID:"+search.w3GetField(COLLECTION,"INPUTDAT",i)+"_"+search.w3GetField(COLLECTION,"INFODIVI",i)+"_"+search.w3GetField(COLLECTION,"CLASCODE",i)+"_"+search.w3GetField(COLLECTION,"NEWSNUMB",i)+"---dupCount:"+dupCount);

      if (dupCount>0 || ISAD.equals("Y")){
 //중복뉴스와 광고성 뉴스는 DB입력 항목에서 제외
           dupTotalCount++;
         logger.debug("Duplicate/Advertising News:"+search.w3GetField(COLLECTION,"DOCID",i));
         
      }else{
         logger.debug("No Duplicate News");      
         result = new Hashtable<String,String>();         
         for (String doc_str : ARR_DOCUMENT_FIELD){
		   //키워드 필드면, MTS사용자가 입력한 키워드와 제목+본문의 명사형 단어와 비교 및 매칭된 목록만 입력
		   if(doc_str.equals("KEYWORD")){
				result.put(doc_str,getMatchUserKeyword(search.w3GetField(COLLECTION,doc_str,i),conn));
		   }else{
			    result.put(doc_str,search.w3GetField(COLLECTION,doc_str,i));
		   }
         }
         alResult.add(result);
      }        

      }

      
    }else{           //뉴스데이터가 색인이 완료되어 검색기에 미반영이면
//         logger.info("There is no News..");    
    }
  logger.info("*Duplicate/Advertising News Count:"+dupTotalCount);
  return alResult;
}

//SCD파일에서 DOCID키값만 추출하는 모듈
public static ArrayList<String> SCDReadDOCID(String SCDfile)throws IOException
{
        String DOCID="";
        BufferedReader br = null;
        ArrayList<String> al = new ArrayList<String>();

        try
        {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(SCDfile), "UTF-8"));        
            String line;
            
            while( true )
            {
                line = br.readLine();
                if( line == null )
                    break;
                
                if( line.startsWith("<DOCID>") )
                {   
                    DOCID = line.substring(7);
                    al.add(DOCID);
                }                            
            }
        }catch( Exception e )
        {
            if( br != null )
                br.close();
        }finally{
            if( br != null )
                br.close();        
        }
      return al;
}

// 수집/색인이 완료된 scd/backup내 dynamic으로 색인완료된 목록중 한개의 SCD만 가져오는 모듈
public static String getFileList(String dirName){
File dirFile = new File(dirName);

SCDFileFilter dynamicSCD = new SCDFileFilter();

File[] fileList = dirFile.listFiles(dynamicSCD);
Arrays.sort(fileList);

String firstSCDFileName="";
  for (File tempFile:fileList){
    firstSCDFileName = tempFile.getName();
    break;
  }
  return firstSCDFileName;
}


//처리완료된 SCD파일을 SCDSourcePath --> SCDTargetPath 이동처리
public static void moveSCDToTarget(String SCDSourcePath , String SCDTargetPath , String SCDFile) throws Exception
{
  File SCDTargetPathDir = new File(SCDTargetPath);
  
  if (!SCDTargetPathDir.exists()){
       SCDTargetPathDir.mkdirs();
  }


  String cmd="mv "+SCDSourcePath+SCDFile+" "+SCDTargetPath;
  //logger.info("cmd:"+cmd);
  try{
    Process p = Runtime.getRuntime().exec(cmd);
  
  }catch(Exception e){
    logger.error("moveSCDToTarget Error:"+e.getMessage());
  }
}


//중복실행을 방지를 위한 모듈
public static boolean isRunning() throws Exception
{
  boolean dupRunChk = false;
  File targetFile = new File(targetFilePath);
  
  if (targetFile.isFile()) dupRunChk=true;
  else{
    String cmd ="touch "+targetFilePath;
    try{
      Process p = Runtime.getRuntime().exec(cmd);
      dupRunChk=false;
    }catch(Exception e){
      logger.error("isRunning Error:"+e.getMessage());
    }  
  }
 
  return dupRunChk;
}

//정상적 종료처리
public static void runFinish()
{
  File targetFile = new File(targetFilePath);
  
    String cmd ="rm -rf "+targetFilePath;
    try{
      Process p = Runtime.getRuntime().exec(cmd);
    }catch(Exception e){
      logger.error("runFinish Error:"+e.getMessage());
    }finally{
      System.exit(1);
    }
}


//환경설정파일 읽어서 변수에 반영
public static void readProperty() throws Exception{

  File pFile = new File(properityFile);  
  
  FileOutputStream fos = null;
  FileInputStream fis = null;
  Properties prop = new Properties();
  
  //설정파일이 없으면 기본정보로 설정파일 내용을 채운다.  
  if (!pFile.isFile()){
    prop.setProperty("COLLECTION",COLLECTION);  
    prop.setProperty("SERVER_IP",SERVER_IP);      
    prop.setProperty("SERVER_PORT",Integer.toString(SERVER_PORT));      
    prop.setProperty("SERVER_TIMEOUT",Integer.toString(SERVER_TIMEOUT));      
    prop.setProperty("DOCUMENT_FIELD",DOCUMENT_FIELD);      
    prop.setProperty("DB_FIELD",DB_FIELD);      
    prop.setProperty("SCDBackupPath",SCDBackupPath);      
    prop.setProperty("DupCheckBackupPath",DupCheckBackupPath);      
    prop.setProperty("DupCheckErrorPath",DupCheckErrorPath);      
    prop.setProperty("dataSourcePath",dataSourcePath);      
    prop.setProperty("dsn",dsn);          
    prop.setProperty("targetFilePath",targetFilePath); 
  
    try{
         fos = new FileOutputStream(properityFile);
         prop.store(fos,"News Duplicate Check Module Properties"); 
     }catch(Exception e){
      logger.error("Function readProperty Write Error:"+e.getMessage());
     }finally{
       if (fos != null) fos.close();
     }
  } //end of if (!pFile.isFile())
  

    try{
         fis = new FileInputStream(properityFile);
         prop.load(fis); 
         
          //검색엔진 관련정보 설정
          COLLECTION=(String)prop.getProperty("COLLECTION");
          SERVER_IP=(String)prop.getProperty("SERVER_IP");
          
          SERVER_PORT=Integer.parseInt((String)prop.getProperty("SERVER_PORT"));
          SERVER_TIMEOUT=Integer.parseInt((String)prop.getProperty("SERVER_TIMEOUT"));
          
          //검색엔진에 정의된 필드목록
          DOCUMENT_FIELD=(String)prop.getProperty("DOCUMENT_FIELD");
          // DB에 정의된 필드목록(검색엔진 필드목록과 순서가 동일해야함)
          DB_FIELD=(String)prop.getProperty("DB_FIELD");
        
          //뉴스기사를 읽어들일 디렉토리 위치  
          SCDBackupPath=(String)prop.getProperty("SCDBackupPath");
          //처리완료 후 저장할 백업디렉토리 위치
          DupCheckBackupPath=(String)prop.getProperty("DupCheckBackupPath");
          //오류발생시 저장할 디렉토리 위치
          DupCheckErrorPath=(String)prop.getProperty("DupCheckErrorPath");
        
          //DB관련 설정
          // DB접속 정보가 저장된 설정파일 지정
          dataSourcePath=(String)prop.getProperty("dataSourcePath");
          dsn=(String)prop.getProperty("dsn");
          
          //중복실행 방지를 위한 타켓파일
          targetFilePath =(String)prop.getProperty("targetFilePath");         
         
     }catch(Exception e){
      logger.error("Function readProperty Read Error:"+e.getMessage());
     }finally{
       if (fis != null) fis.close();
     }

} // End of readProperty()



} //End of Class



