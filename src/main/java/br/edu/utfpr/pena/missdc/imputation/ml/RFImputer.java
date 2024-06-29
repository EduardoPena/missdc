package br.edu.utfpr.pena.missdc.imputation.ml;

import br.edu.utfpr.pena.missdc.input.Table;
import br.edu.utfpr.pena.missdc.input.columns.CategoricalColumn;
import br.edu.utfpr.pena.missdc.input.columns.Column;
import br.edu.utfpr.pena.missdc.utils.misc.MapUtils;
import smile.base.cart.SplitRule;
import smile.classification.RandomForest;
import smile.data.formula.Formula;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.selection.Selection;

import java.util.*;

public class RFImputer {
    tech.tablesaw.api.Table dfTable;
    Table dirtyTable;
    String[][] imputedDataset;
    Set<Column> nonImputedCols;
    Map<String, Integer> stringToIntegerMap;
    Map<Integer, String> integerToStringMap;
    Map<String, Integer> numericStringToIntegerMap;
    Map<Integer, String> numericIntegerToStringMap;
    public RFImputer(Table dirtyTable, String[][] imputedDataset, Set<Column> nonImputedCols) {
        this.dirtyTable = dirtyTable;
        this.imputedDataset = imputedDataset;
        this.nonImputedCols = nonImputedCols;

    }
    public void imputeMissing() {
        mapStringsToIntegers();
        imputeWithRFModels();
        transformPredictionsToOriginalStrings();
    }

    private void transformPredictionsToOriginalStrings() {

        for (Column col : nonImputedCols) {

            IntColumn intCol = dfTable.intColumn(col.ColumnName);

            if (col instanceof CategoricalColumn) {

                for (int tid = 0; tid < dirtyTable.getNUM_RECORDS(); tid++) {
                    if (imputedDataset[tid][col.ColumnIndex] == null) {
                        Integer imputedValue = intCol.get(tid);
                        if (imputedValue != null) {
                            imputedDataset[tid][col.ColumnIndex] = integerToStringMap.get(imputedValue);
                        }
                    }
                }

            } else {
                for (int tid = 0; tid < dirtyTable.getNUM_RECORDS(); tid++) {
                    if (imputedDataset[tid][col.ColumnIndex] == null) {
                        Integer imputedValue = intCol.get(tid);
                        if (imputedValue != null) {
                            imputedDataset[tid][col.ColumnIndex] = numericIntegerToStringMap.get(imputedValue);
                        }
                    }
                }
            }
        }
    }

    private void imputeWithRFModels() {

        for (Column c : nonImputedCols) {
            if (c.getDomain().size() < 2) continue;

            String col = c.ColumnName;

            Selection sel = dfTable.intColumn(col).isMissing();
            tech.tablesaw.api.Table nullPart = dfTable.where(sel);

            if (nullPart.isEmpty()) {
                continue;
            }

            tech.tablesaw.api.Table nonNullT = dfTable.where(dfTable.intColumn(col).isNotMissing());

            try {
                RandomForest RFModel1 = smile.classification.RandomForest.fit(Formula.lhs(col), nonNullT.smile().toDataFrame(), 50, //n
                        (int) Math.sqrt(nonNullT.columnCount() - 1), //m = sqrt(p)
                        SplitRule.GINI, 7, //d
                        100, //maxNodes
                        1, 1);
                int[] predictions = RFModel1.predict(nullPart.smile().toDataFrame());

                int predid = 0;
                for (int tid : sel) {
                    dfTable.intColumn(col).set(tid, predictions[predid]);
                    predid++;
                }
            } catch (Exception e) {
                continue;
            }

        }
    }

    private void mapStringsToIntegers() {

        int nrows = dirtyTable.getNUM_RECORDS();

        stringToIntegerMap = new HashMap<>();
        numericStringToIntegerMap = new HashMap<>();

        int integerRepresentation = 0;

        dfTable = tech.tablesaw.api.Table.create(dirtyTable.NAME);

        List<IntColumn> intcols = new ArrayList<>();
        for (Column col : dirtyTable.getAllColumns().values()) {

            int colid = col.ColumnIndex;
            IntColumn intCol = IntColumn.create(col.ColumnName);

            if (col instanceof CategoricalColumn) {

                for (int tid = 0; tid < nrows; tid++) {
                    String value = imputedDataset[tid][colid];
                    if (value == null) {
                        intCol.appendMissing();
                        continue;
                    }
                    Integer mappedValue = stringToIntegerMap.get(value);
                    if (mappedValue == null) {
                        mappedValue = integerRepresentation++;
                    }
                    stringToIntegerMap.put(value, mappedValue);
                    intCol.append(mappedValue);
                }

            } else {//numerical columns

                for (int tid = 0; tid < nrows; tid++) {
                    String value = imputedDataset[tid][colid];
                    if (value == null) {
                        intCol.appendMissing();
                        continue;
                    }
                    Integer mappedValue = numericStringToIntegerMap.get(value);
                    if (mappedValue == null) {
                        float floatValue = Float.parseFloat(value);
                        mappedValue = Float.floatToIntBits(floatValue);
                    }
                    numericStringToIntegerMap.put(value, mappedValue);
                    intCol.append(mappedValue);
                }

            }

            dfTable.addColumns(intCol);

        }
        integerToStringMap = MapUtils.invertMap(stringToIntegerMap);
        numericIntegerToStringMap = MapUtils.invertMap(numericStringToIntegerMap);

    }
}