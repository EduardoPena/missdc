package br.edu.utfpr.pena.missdc.imputation.ml;

import java.util.ArrayList;
import java.util.List;

public class StringToIntegerMapping {
    public static Integer[][] mapStringToInteger(String[][] imputedDataset) {
        // Create a list to store unique strings and their corresponding integer values
        List<String> uniqueStrings = new ArrayList<>();

        // Convert the imputedDataset to Integer[][] using the mapping
        Integer[][] integerDataset = new Integer[imputedDataset.length][];
        for (int i = 0; i < imputedDataset.length; i++) {
            integerDataset[i] = new Integer[imputedDataset[i].length];
            for (int j = 0; j < imputedDataset[i].length; j++) {
                String str = imputedDataset[i][j];
                if (isNumeric(str)) { // Check if the string is numeric
                    integerDataset[i][j] = Integer.parseInt(str);
                } else {
                    int index = uniqueStrings.indexOf(str);
                    if (index == -1) { // String not found
                        uniqueStrings.add(str);
                        integerDataset[i][j] = uniqueStrings.size() - 1;
                    } else {
                        integerDataset[i][j] = index;
                    }
                }
            }
        }

        return integerDataset;
    }

    // Helper method to check if a string is numeric
    private static boolean isNumeric(String str) {
        try {
            Integer.parseInt(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }


}