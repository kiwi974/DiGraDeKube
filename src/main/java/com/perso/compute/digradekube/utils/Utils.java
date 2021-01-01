package com.perso.compute.digradekube.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class Utils {

    public static Map<Integer, String> stringToHashmap(String stringMap) {
        if (!stringMap.equals("{}")) {
            return Arrays.asList(stringMap.substring(1, stringMap.length()-1).split(","))
                    .stream().map(s -> s.split("=")).collect(Collectors.toMap(e -> Integer.parseInt(e[0].trim()), e -> e[1].trim()));
        } else {
            return Collections.emptyMap();
        }
    }


}
