package utils;

/**
 * Created by Nam on 1/28/2018.
 */
public class EscapseUtils {
    public static String escapseXML(String input) {
        return input.replaceAll("\"", "&quot;").replaceAll("'", "&apos;").replaceAll("<", "&lt;").replaceAll(">", "&gt;").replaceAll("&", "&amp;");
    }
}
