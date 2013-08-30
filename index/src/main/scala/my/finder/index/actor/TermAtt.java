package my.finder.index.actor;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class TermAtt {
    public static  List<String> termAttStr(String aliasName){
        TokenStream stream = null;
        try {
            List<String> list = new ArrayList<String>();
            Version matchVersion  = Version.LUCENE_43;
            StandardAnalyzer analyzer  = new StandardAnalyzer(matchVersion);
            stream = analyzer.tokenStream("field", new StringReader(aliasName));
            CharTermAttribute termAtt  = stream.addAttribute(CharTermAttribute.class);
            stream.reset();
            while ( stream.incrementToken() ) {
               list.add( termAtt.toString() );
            }
            return list;
        } catch (Exception e){
            e.printStackTrace();
            return null;
        } finally {
            try{
                stream.close();
            }catch (IOException ie){
                ie.printStackTrace();
            }
        }
    }
}
