import java.io.File;
import java.io.FilenameFilter;

/**
*  file: SCDFileFilter.jsp
*  subject: SCD 파일중 U-C.SCD 파일 필터링 클래스
*/
public class SCDFileFilter implements FilenameFilter {

  public boolean accept(File dir , String name){
    return name.endsWith("U-C.SCD");
  }

}
