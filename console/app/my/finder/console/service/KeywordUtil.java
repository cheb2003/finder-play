package my.finder.console.service;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeywordUtil {
    public static String normalizeKeyword(String keyword){
        if( StringUtils.isBlank( keyword ) ){
            return null;
        }
        if( keyword.equals("undefined") || keyword.length() < 3 ){
            return null;
        }
        Pattern pattern  = Pattern.compile("^[a-zA-Z\\s0-9]+$");
        String s = keyword.toLowerCase().trim();
        Matcher matcher =  pattern.matcher(s);
        if( matcher.matches() ){
            return s;
        }else{
            return null;
        }
    }
}