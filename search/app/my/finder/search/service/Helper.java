package my.finder.search.service;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;

import java.util.List;

/**
 *
 */
public class Helper {
    public static Sort  addSortField(List<SortField> fields){
        return new Sort(fields.toArray(new SortField[]{}));
    }
}
