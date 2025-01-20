package movieRatings;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {

        String test1 = "167\tShrek\t1\t881964812";
        String test2 = "195\tTerminator 2\t2\t883991909";
        String test3 = "232\tMission Impossible 2\t4\t860226460";

        String[] res1 = test1.split("\t");
        String[] res2 = test2.split("\t");
        String[] res3 = test3.split("\t");

        System.out.println(Arrays.toString(res1));
        System.out.println(Arrays.toString(res2));
        System.out.println(Arrays.toString(res3));

        System.out.println(res1[2]);
        System.out.println(res2[2]);
        System.out.println(res3[2]);

    }
}