package br.edu.utfpr.pena.missdc.input;

import br.edu.utfpr.pena.missdc.input.columns.CategoricalColumn;
import br.edu.utfpr.pena.missdc.input.columns.NumericalColumn;
import br.edu.utfpr.pena.missdc.input.columns.Column;
import br.edu.utfpr.pena.missdc.utils.misc.UnsortedObject2FloatMapProvider;
import it.unimi.dsi.fastutil.floats.Float2ObjectMap;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;

public class Table {

    public final File DATASETFILE;
    public final String NAME;
    public final int NUM_ORIGINAL_COLLUMNS;
    private final Map<Integer, Column> allColumns;
    private final Map<Integer, NumericalColumn> numericalColumns;
    private final Map<Integer, CategoricalColumn> categoricalColumns;
    protected Set<Column> requiredColumns;
    List<Column> colsWithNulls;
    private int NUM_RECORDS;
    private Map<String, Column> nameToColumnMap;

    public Table(File datasetFile, String tableName, int nCols, Map<Integer, Column> allColumns,
                 Map<Integer, NumericalColumn> numericalColumns, Map<Integer, CategoricalColumn> categoricalColumns) {

        this.DATASETFILE = datasetFile;
        this.NAME = tableName;
        this.NUM_ORIGINAL_COLLUMNS = nCols;
        this.allColumns = allColumns;
        this.numericalColumns = numericalColumns;
        this.categoricalColumns = categoricalColumns;

        mapColumnNameToColumn();

    }

    public Table(File datasetFile, String tableName, int nCols, int nRows, Map<Integer, Column> allColumns,
                 Map<Integer, NumericalColumn> numericalColumns, Map<Integer, CategoricalColumn> categoricalColumns) {

        this.DATASETFILE = datasetFile;
        this.NAME = tableName;
        this.NUM_ORIGINAL_COLLUMNS = nCols;
        this.NUM_RECORDS = nRows;
        this.allColumns = allColumns;
        this.numericalColumns = numericalColumns;
        this.categoricalColumns = categoricalColumns;

        mapColumnNameToColumn();

    }

    public Map<Integer, Column> getAllColumns() {
        return allColumns;
    }

    public Map<Integer, NumericalColumn> getNumericalColumns() {
        return numericalColumns;
    }

    public Map<Integer, CategoricalColumn> getCategoricalColumns() {
        return categoricalColumns;
    }

    private void mapColumnNameToColumn() {
        nameToColumnMap = new HashMap<>();
        for (Column col : allColumns.values()) {
            nameToColumnMap.put(col.ColumnName, col);

            // System.out.println(col.getName() + ":" + col.getCardinality());
        }
    }

    public Table getBaseTableCopy() {// same values internally

        Map<Integer, Column> allColumnsCopy = new HashMap<>();
        Map<Integer, NumericalColumn> numericalColumnsCopy = new HashMap<>();
        Map<Integer, CategoricalColumn> categoricalColumnsCopy = new HashMap<>();

        for (Entry<Integer, NumericalColumn> entry : numericalColumns.entrySet()) {

            NumericalColumn col = entry.getValue();

            NumericalColumn colCopy = new NumericalColumn(col.TableName, col.ColumnName, col.ColumnIndex,
                    col.ColumnType);

            numericalColumnsCopy.put(col.ColumnIndex, colCopy);
            allColumnsCopy.put(col.ColumnIndex, colCopy);

        }

        for (Entry<Integer, CategoricalColumn> entry : categoricalColumns.entrySet()) {

            CategoricalColumn col = entry.getValue();

            CategoricalColumn colCopy = new CategoricalColumn(col.TableName, col.ColumnName, col.ColumnIndex,
                    col.ColumnType);

            categoricalColumnsCopy.put(col.ColumnIndex, colCopy);
            allColumnsCopy.put(col.ColumnIndex, colCopy);

        }

        return new Table(this.DATASETFILE, this.NAME, this.NUM_ORIGINAL_COLLUMNS, allColumnsCopy, numericalColumnsCopy,
                categoricalColumnsCopy);

    }

    public Map<String, Column> getNameToColumnMap() {
        return nameToColumnMap;
    }

    public void printColumnsValues() {
        for (int i = 0; i < NUM_ORIGINAL_COLLUMNS; i++) {
            System.out.println(getAllColumns().get(i) + ":" + getAllColumns().get(i).getValuesList());
        }
        System.out.println();
    }

    public int getNUM_RECORDS() {
        return NUM_RECORDS;
    }

    public void setNUM_RECORDS(int nUM_RECORDS) {
        NUM_RECORDS = nUM_RECORDS;
    }

    public Column getColumnByName(String colName) {

        return nameToColumnMap.get(colName.toUpperCase());
    }

    public Set<Column> getRequiredColumns() {
        return requiredColumns;
    }

    public void setRequiredColumns(Set<Column> requiredColumns) {
        this.requiredColumns = requiredColumns;
    }

    public Table deepCopyTableValues() {

        Map<Integer, Column> allColumnsCp = new HashMap<>();
        Map<Integer, NumericalColumn> numericalColumnsCp = new HashMap<>();
        Map<Integer, CategoricalColumn> categoricalColumnsCp = new HashMap<>();

        for (Entry<Integer, Column> col : allColumns.entrySet()) {

            Column originalCol = col.getValue();
            int originalColId = col.getKey();

            if (originalCol instanceof CategoricalColumn originalCatCol) {

                UnsortedObject2FloatMapProvider<String> originalfloatDictionary = originalCatCol.getFloatDictionary();
                FloatList originalValuesList = originalCol.getValuesList();

                UnsortedObject2FloatMapProvider<String> copiedfloatDictionary = originalfloatDictionary.deepCopy();
                FloatList copiedValuesList = new FloatArrayList(originalValuesList);

                CategoricalColumn copiedCatCol = new CategoricalColumn(this.NAME, originalCol.ColumnName, originalColId,
                        originalCol.ColumnType, copiedfloatDictionary, copiedValuesList);

                categoricalColumnsCp.put(originalColId, copiedCatCol);
                allColumnsCp.put(originalColId, copiedCatCol);

            } else if (originalCol instanceof NumericalColumn) {

                FloatList originalValuesList = originalCol.getValuesList();
                FloatList copiedValuesList = new FloatArrayList(originalValuesList);

                NumericalColumn copiedNumCol = new NumericalColumn(this.NAME, originalCol.ColumnName, originalColId,
                        originalCol.ColumnType, copiedValuesList);

                numericalColumnsCp.put(originalColId, copiedNumCol);
                allColumnsCp.put(originalColId, copiedNumCol);

            }

        }

        Table copied = new Table(this.DATASETFILE, this.NAME, this.NUM_ORIGINAL_COLLUMNS, this.NUM_RECORDS,
                allColumnsCp, numericalColumnsCp, categoricalColumnsCp);

        return copied;
    }

    public String toSimpleTableValueString() {
        StringBuilder sb = new StringBuilder();

        for (Entry<Integer, Column> colEntry : allColumns.entrySet()) {

            Column col = colEntry.getValue();
            int colId = colEntry.getKey();

            FloatList valuesList = col.getValuesList();

            sb.append(colId + "=" + col.ColumnName + "=" + valuesList);
            sb.append("\n");

        }

        return sb.toString();
    }

    public String[][] getCells() {

        String[][] cells = new String[getNUM_RECORDS()][getAllColumns().size()];

        for (Entry<Integer, CategoricalColumn> colEntry : categoricalColumns.entrySet()) {
            int colId = colEntry.getKey();
            CategoricalColumn col = colEntry.getValue();
            Float2ObjectMap<String> reverseDictionary = col.buildReverseDictionaryMap();
            FloatList valuesList = col.getValuesList();

            for (int tid = 0; tid < valuesList.size(); tid++) {
                cells[tid][colId] = reverseDictionary.get(valuesList.getFloat(tid));
            }
        }

        for (Entry<Integer, NumericalColumn> colEntry : numericalColumns.entrySet()) {
            int colId = colEntry.getKey();
            NumericalColumn col = colEntry.getValue();

            FloatList valuesList = col.getValuesList();

            for (int tid = 0; tid < valuesList.size(); tid++) {
                cells[tid][colId] = "" + valuesList.getFloat(tid);
            }
        }


        return cells;
    }

    public List<Column> getColsWithNulls() {


        if (colsWithNulls != null) {
            return colsWithNulls;
        }


        colsWithNulls = new ArrayList<>();
        for (Column col : allColumns.values()) {
            if (!col.getTIDsMissing().isEmpty()) {

                colsWithNulls.add(col);

            }
        }

        return colsWithNulls;
    }

}