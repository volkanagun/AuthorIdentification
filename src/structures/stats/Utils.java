package structures.stats;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wolf on 19.07.2015.
 */
public class Utils {


    public static <K,V> void addMapList(Map<K, List<V>> map, K key, V value){
        if(map.containsKey(key)){
            map.get(key).add(value);
        }
        else{
            List<V> values = new ArrayList<>();
            values.add(value);
            map.put(key,values);
        }
    }

    public static double charLength(String value){
        return value.length();
    }

    public static double wordLength(String value){
        return value.split("\\s+").length;
    }
}
