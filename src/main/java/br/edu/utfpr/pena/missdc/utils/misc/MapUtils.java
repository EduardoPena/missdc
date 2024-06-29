package br.edu.utfpr.pena.missdc.utils.misc;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {

    public static Map<Integer, String> invertMap(Map<String, Integer> originalMap) {
        Map<Integer, String> invertedMap = new HashMap<>();
        for (Map.Entry<String, Integer> entry : originalMap.entrySet()) {
            invertedMap.put(entry.getValue(), entry.getKey());
        }
        return invertedMap;
    }
}