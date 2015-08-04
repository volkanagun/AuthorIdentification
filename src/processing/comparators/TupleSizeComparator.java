package processing.comparators;

import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by wolf on 01.08.2015.
 */
public class TupleSizeComparator implements Comparator<Tuple2<String, Long>>, Serializable {

    @Override
    public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
        if (o1._2() > o2._2()) return 1;
        else if (o1._2() < o2._2()) return -1;
        else return 0;
    }
}
