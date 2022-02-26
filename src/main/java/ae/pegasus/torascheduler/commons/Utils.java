
package ae.pegasus.torascheduler.commons;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Utils {


    public static List<String> SplitAtOccurence(String[] parts, int occurence)
    {
//        String[] parts = input.split(",");
        List<String> partlist = new ArrayList<String>();
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < parts.length; i++)
        {
            if (partlist.size() == occurence)
            {
                result.add(String.join(",", partlist));
                partlist.clear();
            }
            partlist.add(parts[i]);
            if (i == parts.length - 1) result.add(String.join(",", partlist)); // if no more parts, add the rest
        }
        return result;
    }


    public static <T> List<List<T>> NSizeParts(List<T> objs, int N) {
        return new ArrayList<>(IntStream.range(0, objs.size()).boxed().collect(
                Collectors.groupingBy(e->e/N,Collectors.mapping(e->objs.get(e), Collectors.toList())
                )).values());
    }

    public static Map<String, String> sinceUntilUTC(int lastXMinutes)
    {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        Date date = new Date();
        String until = dateFormat.format(date);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, -lastXMinutes);
        String since = dateFormat.format(calendar.getTime());
        Map<String, String> sinceUntil = new HashMap<>();

        sinceUntil.put("since", since);
        sinceUntil.put("until", until);

        return sinceUntil;
    }

}




