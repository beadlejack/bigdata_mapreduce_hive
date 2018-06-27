// Immanuel Amirtharaj, Jackson Beadle
// COEN 242 -- Project 1
// Part 1
// Comparator class for intermediate results in Job2 of Popularity class

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


// comparator class used in job2 to sort the intermediate results
// want to prevent movies with the same reviewCount from being mapped to the same key
// used a combinator key with "reviewCount\tTitle"
// sort by reviewCount, if same reviewCount, then sort by title
// ascending order
public class PopularityComparator extends WritableComparator {

    protected PopularityComparator() {
        super(Text.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {

        // get reviewCount and title from both keys
        String[] keys1 = w1.toString().split("\t");
        String[] keys2 = w2.toString().split("\t");

        // get reviewCounts
        Integer count1 = Integer.parseInt(keys1[0]);
        Integer count2 = Integer.parseInt(keys2[0]);


        Integer reviews = count1 - count2;
        if (reviews == 0) {
            // if reviewCounts are equal, compare titles
            return keys1[1].compareTo(keys2[1]);
        }
        else
            // return comparison on reviewCount
            return reviews;
    }
}
