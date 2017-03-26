import kr.co.wisenut.bridge3.config.source.MemorySelect;
import kr.co.wisenut.bridge3.config.source.SubQuery;
import kr.co.wisenut.bridge3.job.custom.ISubCustom;
import kr.co.wisenut.common.util.HtmlUtil;
import kr.co.wisenut.common.Exception.CustomException;
import kr.co.wisenut.common.util.HtmlTagRemover;
import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SKStockCustom implements ISubCustom
{
    static Hashtable<String,String> HM;  
    public String customData( String str, SubQuery subQuery ) throws CustomException
    {
       String customValue = "";
       String strTitle = "";
       String strContent = "";
       String remveHtmlTagstr = "";
       
       String dupwordstr="";       
       String topicwordstr=""; 
                    
       Hashtable<String,String> ht = new Hashtable<String,String>();
       
       HtmlTagRemover htr = new HtmlTagRemover();
       
       if (str.indexOf("~split~")!=-1){
       strTitle = str.substring(0,str.indexOf("~split~"));
       remveHtmlTagstr = htr.removeHtmlTag(strTitle);//HTML 태그제거
       strTitle=remveHtmlTagstr;
       strContent = str.substring(str.indexOf("~split~")+7,str.length());     
	   strContent = removeStyle(strContent); // Style Sheet 제거
       remveHtmlTagstr = htr.removeHtmlTag(strContent);//HTML 태그제거 
       strContent=remveHtmlTagstr;             
      //KMA를 이용한 핵심단어 추출
      SKNewsKMAKeywordExtract kma = new SKNewsKMAKeywordExtract();            


      dupwordstr = kma.extract(strTitle,strContent, 6,2);
      topicwordstr = kma.extract(strTitle,strContent, 6,0);               

/*
	  //뉴스기사중 기사내용이 숫자가 많이 포함된 수치가 포함된 도표로 되어 있는 경우 핵심단어가 숫자만 나올 확률이 높아,
	  //핵심단어가 숫자만 추출되는 경우 제목만으로 핵심단어를 추출함(예: 투자별 동향에 관련한 뉴스일 경우)
	  if (isNumber(dupwordstr.replaceAll(" ",""))){
			dupwordstr=kma.extract(strTitle,"", 3,1);
	  }

	  //뉴스기사중 기사내용이 숫자가 많이 포함된 수치가 포함된 도표로 되어 있는 경우 핵심단어가 숫자만 나올 확률이 높아,
	  //핵심단어가 숫자만 추출되는 경우 제목만으로 핵심단어를 추출함(예: 투자별 동향에 관련한 뉴스일 경우)
	  if (isNumber(topicwordstr.replaceAll(" ",""))){
			topicwordstr=kma.extract(strTitle,"", 3,1);
	  }
*/
      }else{
        dupwordstr="@Extract Fail@";
        topicwordstr="@Extract Fail@";
       }
       
       str=strContent;
       str+="\n";
       str+="<DUPWORD>";              
       str+=dupwordstr;
       str+="\n";
       str+="<TOPICWORD>";
       str+=topicwordstr;
       str+="\n";
       
      //광고성 체크 문자열을 가져옴
      if (HM==null) {
          HM=ReadADWordList();
      }
       ht = CheckAD(str,HM);
       str+="<ISAD>";
       str+=ht.get("ISAD");
       str+="\n";       
       str+="<ADREASON>";
       str+=ht.get("ADREASON");       

       customValue = str;

        return customValue;
    }

	public boolean isNumber(String str){
		boolean flag = Pattern.matches("^[0-9]*$",str);
		return flag;

	}



    public String customData( String str, MemorySelect subMemory ) throws CustomException
    {
        return null;
    }
    
  //광고성 단어 목록을 row단위로 비교하여 하나의 row에 등록된 광고성 단어가 모두 일치하면 광고로 판단.
	public Hashtable<String,String> CheckAD(String input , Hashtable<String,String> rule) {
		
		Hashtable<String,String> ht = new Hashtable<String,String>();
		input = input.toUpperCase();
		boolean isAD =false;
		int matchCount=0;
		
		String recommendKey="";
		
		  Enumeration<String> enumerationKey = rule.keys();
		  while(enumerationKey.hasMoreElements()){
		  recommendKey= enumerationKey.nextElement().toString();	
		  String recommendValue= rule.get(recommendKey).toString();			  
 		  String[] wordList = recommendValue.split("\\;");
			for (String word:wordList){
				if (input.contains(word.toUpperCase())) matchCount++;
			} 		  
					if (wordList.length == matchCount) {
						isAD=true;
						break;
					}
					matchCount=0;
			} 		  


			if (isAD) {
			  ht.put("ISAD","Y");
			  ht.put("ADREASON",recommendKey); //광고판단 적용 룰
			}else{
			  ht.put("ISAD","N");
			  ht.put("ADREASON",""); //광고판단 적용 룰			  
			}
			
			return ht;
	}    
    
  //    
	public static Hashtable<String,String> ReadADWordList() {    
	  
    
	  String SF1Path="/home/newsfilter";
	  String RecommendPath=SF1Path+"/sf-1/manager/webapps/manager/WEB-INF/cache/recommend";
	  String TargetFile = RecommendPath+"/target.recommend.0";
	  String Target ="0";
	  	  
	  //Recommend Tag을 파일명으로 확인
	  File f = new File(TargetFile);       
	  if (f.isFile()) Target="0";
    else Target="1";
    
    String cacheFile=RecommendPath+"/"+Target+"/recommend.cache";

    BufferedReader br = null;
    ArrayList<String> recommendList = new ArrayList<String>();
    String line;    
    try {
      br = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFile), "UTF-8"));
      
      while( true )
      {
          line = br.readLine();
          if( line == null )  break;   
          recommendList.add(line);
       }
    }catch(Exception e){}	    
	  
	  Hashtable<String,String> M_RecommendRow= new Hashtable<String,String>();
  
 
	  for (String RecommendRow : recommendList){
      String[] RecommendColumn = RecommendRow.split("/");

      String exceptWord=""; 
      for (int i=3 ; i < RecommendColumn.length;i++){
        if (i==3) exceptWord+=RecommendColumn[i];
        else exceptWord+=";"+RecommendColumn[i];      
      }
      
      M_RecommendRow.put(RecommendColumn[0],exceptWord);      
    }
    
    return M_RecommendRow;

	  
	} // End of Function    


//<style></style>태그내 문자제거
	public static String removeStyle(String str){
	
	String value="";
	Matcher mat;
	Pattern style = Pattern.compile("<style[^>]*>.*</style>",Pattern.DOTALL);
	mat = style.matcher(str.toLowerCase());
	value=mat.replaceAll(" ");
	return value;

	}
    
    
    
    
}
