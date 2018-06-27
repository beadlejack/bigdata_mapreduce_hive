// Immanuel Amirtharaj, Jackson Beadle
// COEN 242 -- Project 1
// Part 2
// Comparator class to sort intermediate results in Job2 of Reviews class

import org.apache.hadoop.io.*;

// comparator class used in job2 to sort the intermediate results
// want to prevent movies with the same title from being mapped to the same key
// used a combinator key with "title\taverageRating"
// sort by averageRating, if same averageRating, then sort by title
// ascending order
public class ReviewComparator extends WritableComparator {

    protected ReviewComparator() {
        super(Text.class, true);
    }


    // title    averageRating   reviewCount
    public int compare(WritableComparable w1, WritableComparable w2) {

        String[] keys1 = w1.toString().split("\t", 2);
        String[] keys2 = w2.toString().split("\t", 2);


        double score1 = Double.parseDouble(keys1[1]);
        double score2 = Double.parseDouble(keys2[1]);

        double scoreComp = score1 - score2;
        if (scoreComp == 0) {
            // if same averageRating, compare titles
            return keys1[0].compareTo(keys2[0]);
        }
        else {
            // return comparison of averageRatings
            if (scoreComp > 0) {
                return 1;
            }
            else {
                return -1;
            }
        }
    }
}
