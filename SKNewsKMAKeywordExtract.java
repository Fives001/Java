
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import kr.co.wisenut.wisekmaorange.WK_Analyzer;
import kr.co.wisenut.wisekmaorange.WK_Eojul;
import kr.co.wisenut.wisekmaorange.WK_Global;
import kr.co.wisenut.wisekmaorange.WK_Knowledge;

public class SKNewsKMAKeywordExtract {
    
    private static final String KNOWLEDGE_PATH = "/home/newsfilter/sf-1/knowledge/korean";
//    private static final String KNOWLEDGE_PATH = "/data1/cse/hanwool/build/sf-1-v5.0.0-daeshin-r6631/knowledge/korean";
    private static WK_Knowledge knowledge = null;
    private static WK_Analyzer analyzer = null;
    private static WK_Eojul eojul = null;
    private static Set<String> stopword = null;
    
    private static void checkKnowledge()
    {
        int ret;
        
        if( knowledge == null )
        {
            knowledge = new WK_Knowledge();
            knowledge.createObject();

            // Load System Dictionary
            ret = knowledge.loadKnowledge( KNOWLEDGE_PATH, true, false );
            if( ret < 0 )
            {
                System.err.println( "Knowledge loading faild!!!" );
                return;
            }
            
            // Create Analysis
            analyzer = new WK_Analyzer( knowledge );
            
            // Create Eojul
            eojul = new WK_Eojul();
            eojul.createObject();

            // Set Eojul for analyzing
            analyzer.setEojul( eojul );

            // Set Option for Analzyer
            analyzer.setOption( WK_Global.WKO_OPTION_N_BEST, 1 );
            analyzer.setOption( WK_Global.WKO_OPTION_EXTRACT_ALPHA, 1 );
            analyzer.setOption( WK_Global.WKO_OPTION_EXTRACT_NUM, 1 );
            
            // Set stopword
            stopword = new HashSet<String>();
            String[] words = new String[]{"대통령", "총리", "장관", "차관", "의원", "국회의원",
                    "선수", "기자",
                    "회장", "부회장", "사장", "부사장", "상무", "이사", "부장", "차장", "과장", "대리", "사원", "연구원","브랜드"};
            for(int w = 0; w < words.length; w++ )
            {
                stopword.add(words[w]);
            }
        }
    }
    
   
    public List sortByValue(final Map map){
        List<String> list = new ArrayList();
        list.addAll(map.keySet());
         
        Collections.sort(list,new Comparator(){
             
            public int compare(Object o1,Object o2){
                Object v1 = map.get(o1);
                Object v2 = map.get(o2);
                 
                return ((Comparable) v1).compareTo(v2);
            }
             
        });
        Collections.reverse(list); // 주석시 오름차순
        return list;
    }
    
    static Comparator<String> strSort = new Comparator<String>() {
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    };
    
    public String normalize(String text)
    {
        String result = text;
        
        result = result.replaceAll("#[0-9]+;", " ");
        result = result.replaceAll("&[a-z]+;", " ");
        
        while( true )
        {
            int spos = result.indexOf("[");
            if( spos == - 1)
                break;
            
            int epos = result.indexOf("]", spos);
            if( epos == -1 )
                break;
            
            result = result.substring(0, spos) + result.substring(epos+1);
        }
        while( true )
        {
            int spos = result.indexOf("(");
            if( spos == - 1)
                break;
            
            int epos = result.indexOf(")", spos);
            if( epos == -1 )
                break;
            
            result = result.substring(0, spos) + result.substring(epos+1);
        }
        while( true )
        {
            int spos = result.indexOf("<");
            if( spos == - 1)
                break;
            
            int epos = result.indexOf(">", spos);
            if( epos == -1 )
                break;
            
            result = result.substring(0, spos) + result.substring(epos+1);
        }
        
        String pat = "[!@#$%^&\\?\\*\\(\\)_\\-=\\+\\{\\}<>\\.,\\\\/;:\"'~`\\[\\]“”‘’\r\n\t※·ㆍ‥…¨〃­―∥＼∼‘’“”′″〔〕〈〉《》「」『』【】☆★○●◎◇◆□■△▲▽▼∇¤♨☏☎☜☞㉿㈜◁◀▷▶♤♠♡♥♧¡¿～ˇ˘⌒˚♣⊙◈▣◐◑▒▤▥▨▧▦▩㉠㉡㉢㉣㉤㉥㉦㉧㉨㉩㉪㉫㉬㉭㉮㉯㉰㉱㉲㉳㉴㉵㉶㉷㉸㉹㉺㉻ⓐⓑⓒⓓⓔⓕⓖⓗⓘⓙⓚⓛⓜⓝⓞⓟⓠⓡⓢⓣⓤⓥⓦⓧⓨⓩ①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮½⅓⅔¼¾⅛⅜⅝⅞¹²³⁴ⁿ₁₂₃↕↗↙↖↘♭♩♪♬∮§±×÷≠≤≥∞∴≡≒√∽∝∵∫∬∈∋⊆⊇⊂⊃∪∩∧∨∑∠⊥→←↑↓↔⇒⇔♂♀†‡ⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩæΔΦΨΩαβγδεζηθικλμνξοπρστυφχψω㎕㎖㎗ℓ㎘㏄㎣㎤㎥㎦㎛㎟㎠㎡㎢㏊㎍㏈㎧㎨㎰㎶Ω㎮㎯㏆¶℃Å￠￡￥°。‰℉∀∃∏∂]";
        result = result.toLowerCase().replaceAll(pat, " ");
        
        return result;
    }
    
    public void wordNGram(Map<String, Integer> out, List<String> wlist, Map<String, Integer> ref, int n)
    {
        Set<String> checkDup = new HashSet<String>();
        
        for( int w = n-1; w < wlist.size(); w++ )
        {
            String word = new String();
            Integer sum = new Integer(0);
            for( int i = 0; i < n; i++ )
            {
                String temp = wlist.get(w - (n-i-1));
                if( stopword.contains(temp) == true )
                {
                    word = null;
                    break;
                }
                sum += ref.get(temp);
                word += temp;
            }
            
            if( (word != null) && (checkDup.contains(word) == false) )
            {
                checkDup.add(word);
                
                if( out.get(word) == null )
                    out.put(word, sum);
                else
                    out.put(word, sum + out.get(word));
            }
        }
    }
    
    public String topNword(Map<String, Integer> words, int topn, boolean dupfirst)
    {
        List<String> result = new ArrayList<String>(); 
        
        int count = 0;

        Iterator it = this.sortByValue(words).iterator();
        while( it.hasNext() && (count < topn) )
        {
            String temp = (String) it.next();
            if( stopword.contains(temp) == true )
                continue;
            
            count++;
            if( (dupfirst == true) && (count == 1) )
            {
                result.add(temp);
//                output.add(temp + "=" + words.get(temp));
            }
            result.add(temp);
//            output.add(temp + "=" + words.get(temp));
//            System.out.println(temp + " = " + rlist.get(temp));
        }
        
        Collections.sort(result, strSort);
        StringBuffer sb = new StringBuffer();
        for( int i = 0; i < result.size(); i++ )
        {
            sb.append( result.get(i) );
            sb.append(" ");
        }
        return sb.toString();
    }
    
    
// 유입되는 뉴스기사의 주제어를 추출하여 관련뉴스를 체크하기 위해 사용 - 관련뉴스로 묶기 위해 주제어가 복합명사를 구성됨
    public String extract(String title, String content, int titleWordRate , int titleBiWordRate)
    {
        
        String ntitle = normalize(title);
        String ncontent = normalize(content);


        List<String> titleList = new ArrayList<String>();
        Map<String, Integer> titleWord = kmaProcess(ntitle, titleList, true);
        List<String> contentList = new ArrayList<String>();
        Map<String, Integer> contentWord = kmaProcess(ncontent, contentList, false);

        
        for( String key : contentWord.keySet() )
        {
            if( titleWord.get(key) != null )
                titleWord.put(key, titleWord.get(key) + contentWord.get(key) + 5);
            else 
                titleWord.put(key, contentWord.get(key) );
        }
        
        Map<String, Integer> titleBiWord = new HashMap<String, Integer>();
        wordNGram(titleBiWord, titleList, titleWord, 2);
        wordNGram(titleBiWord, contentList, contentWord, 2);


       
        String result = "";
        result += topNword(titleWord, titleWordRate, true);
        result += topNword(titleBiWord, titleBiWordRate, true);

        
        return result;
    }
    

    public Map<String, Integer> kmaProcess(String c, List<String> l, boolean istitle)
    {
        final boolean DEBUG = false;
        
        checkKnowledge();
        
        Map<String, Integer> rlist = new HashMap<String,Integer>();
        
        int ret;
        boolean isfirst = true;
        StringTokenizer st = new StringTokenizer(c, " ");
        while( st.hasMoreTokens() )
        {
            String temp = st.nextToken();
            if( temp.length() <= 0 )
                continue;
            
            // Eojul Initialize
            eojul.eojulInitialize();

            // Set String for analyzing
            eojul.setString( temp );

            // analyzing
            ret = analyzer.runWithEojul();
            if( ret < 0)
            {
                System.err.println( "Analyze Faild: " + eojul.getString() );
                break;
            }

            // Print Result
            if( DEBUG == true )
            {
                System.out.println( "\n> [" + eojul.getString() + "]" );
            }
            
            //
            int inc = 1;
            if( (isfirst == true) && (istitle == true) )
            {
                if( temp.matches(".*[0-9].*") == false)
                {
                    inc = 10;
                    isfirst = false;
                }
            }

            for( int posOfList = 0; posOfList < eojul.getListSize(); posOfList++ )
            {
                String word = "";
                
                for( int posOfMorph = 0; posOfMorph < eojul.getCount(posOfList); posOfMorph++ )
                {
                    if( DEBUG == true )
                    {
                        if( posOfMorph != 0 )
                            System.out.print( " + " );
                        else
                            System.out.print( "\t" + posOfList +" : ");
                    }

                    String morph = eojul.getLexicon(posOfList, posOfMorph);
                    String tag  = eojul.getStrPOS(posOfList, posOfMorph);
                    
                    boolean isindexword = false;
                    if(
                            (tag.equals("XP") == true) ||
                            (tag.equals("XSN") == true) ||
                            (tag.equals("SN") == true) ||
                            (tag.equals("FL") == true) ||
                            (tag.startsWith("N") == true)
                       )
                    {
                        isindexword = true;
                    }

                    if( DEBUG == true )
                    {
                        System.out.print(morph + "/" + tag);
                        if( isindexword == true )
                            System.out.print(" [1]");
                        else
                            System.out.print(" [0]");
                    }
                    
                    if( isindexword == true )
                    {
                        word += morph;
                    }
                    
                    if( isindexword == false )
                    {
                        if( word.length() > 0 )
                        {
                            if( DEBUG == true )
                            {
                                System.out.println("detect[" + word + "]");
                            }
                            if( rlist.get(word) == null )
                                rlist.put(word, inc);
                            else
                                rlist.put(word, rlist.get(word) + inc);
                            l.add(word);
                        }
                        word = "";
                    }
                }

                if( word.length() > 0 )
                {
                    if( DEBUG == true )
                    {
                        System.out.println("detect[" + word + "]");
                    }
                    if( rlist.get(word) == null )
                        rlist.put(word, inc);
                    else
                        rlist.put(word, rlist.get(word) + inc);
                    l.add(word);
                }
                if( DEBUG == true )
                {
                    System.out.println("");
                }
            }
        }
        
        return rlist;
    }
    
    
}