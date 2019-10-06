package packt;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class UtilitiesJava {

    public static StringTokenizer tokenizeSimple(String text) {
        StringTokenizer tokenizer = new StringTokenizer(text);
        return tokenizer;
    }

    public static List<String> createBigrams(String text) {
        List<String> bigrams = new ArrayList<>();
        StringTokenizer tokenizer = tokenizeSimple(text);
        String previousToken = "";
        while (tokenizer.hasMoreTokens()) {
            String currentToken = tokenizer.nextToken();
            if (previousToken.isEmpty()) {
                previousToken = currentToken;
                continue;
            }
            bigrams.add(previousToken + " " + currentToken);
            previousToken = currentToken;
        }
        return bigrams;
    }


    public static void main(String... args) {
        String text = "I had a big steak for lunch today";
        List<String> bigrams = createBigrams(text);
        System.out.println(bigrams);
    }


}
