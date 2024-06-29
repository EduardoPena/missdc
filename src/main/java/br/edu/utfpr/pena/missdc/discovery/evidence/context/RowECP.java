package br.edu.utfpr.pena.missdc.discovery.evidence.context;

import br.edu.utfpr.pena.missdc.input.Table;
import br.edu.utfpr.pena.missdc.dc.predicates.IndexedPredicate;
import br.edu.utfpr.pena.missdc.dc.predicates.groups.ColumnPredicateGroup;
import br.edu.utfpr.pena.missdc.dc.predicates.groups.NumericalColumnPredicateGroup;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.EqualityIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.LtBinnedIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.LtIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.comparators.EqualityIndexSizeComparator;
import br.edu.utfpr.pena.missdc.dc.predicates.space.PredicateSpace;
import br.edu.utfpr.pena.missdc.discovery.evidence.EvidenceSet;
import br.edu.utfpr.pena.missdc.discovery.evidence.ParallelEvidenceSet;
import br.edu.utfpr.pena.missdc.input.columns.CategoricalColumn;
import br.edu.utfpr.pena.missdc.input.columns.Column;
import br.edu.utfpr.pena.missdc.input.columns.NumericalColumn;
import br.edu.utfpr.pena.missdc.input.sorters.ColumnComparatorByCardinality;
import br.edu.utfpr.pena.missdc.input.sorters.RowComparatorMultiColumn;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class RowECP implements IEvidenceSetBuilder {

    private static final int BinThreshold = 2000;
    protected int rowNumber;
    private final Table table;
    private final PredicateSpace predicateSpace;
    private List<ColumnPredicateGroup> catPredGroups;
    private List<NumericalColumnPredicateGroup> numPredGroups;
    private RoaringBitmap nonnullTIDs;
    private ArrayList<Integer> nonnullTIDsList;
    private RoaringBitmap nullTIDs;
    private ArrayList<Integer> nullTIDsList;

    private ParallelEvidenceSet nonNullRowEvidenceSet;// all evidence from non-null rows


    private BitSet baseEvidence;

    public RowECP(Table table, PredicateSpace predicateSpace) {

        this.table = table;
        this.predicateSpace = predicateSpace;
        this.rowNumber = table.getNUM_RECORDS();

    }

    private void fillNonNullAndNullTIDs() {

        nullTIDs = new RoaringBitmap();

        List<Column> colsWithNulls = table.getColsWithNulls();

        for (Column col : colsWithNulls) {

            ColumnPredicateGroup group = predicateSpace.getColumn2GroupMap().get(col);
            BitSet missing = group.getPredicateIDs();

            for (Integer tid : col.getTIDsMissing()) {

                nullTIDs.add(tid);
            }
        }

        nonnullTIDs = new RoaringBitmap();
        nonnullTIDs.add(0, (long) rowNumber);
        nonnullTIDs.andNot(nullTIDs);

        nonnullTIDsList = new ArrayList<>(Arrays.stream(nonnullTIDs.toArray()).boxed().toList());
        nullTIDsList = new ArrayList<>(Arrays.stream(nullTIDs.toArray()).boxed().toList());


    }


    public EvidenceSet build() {

        organizeRowsAndIndexes();

        fillNonNullAndNullTIDs();

        nonNullRowEvidenceSet = new ParallelEvidenceSet(predicateSpace);

        this.baseEvidence = createNonNullBaseEvidence();


        nonnullTIDsList.parallelStream().forEach(tid -> {

            RoaringBitmap tableIdsCopy = nonnullTIDs.clone();
            tableIdsCopy.remove(0L, (tid + 1));

            TupleContextHandler eviContext = new TupleContextHandler(tid, tableIdsCopy, baseEvidence);

            for (ColumnPredicateGroup catGroup : catPredGroups) {
                eviContext.updateCategoricalContext(catGroup);
            }

            for (NumericalColumnPredicateGroup numGroup : numPredGroups) {
                eviContext.updateNumericalContext(numGroup);
            }

            eviContext.collectEvidence(nonNullRowEvidenceSet, numPredGroups);
        });


        return nonNullRowEvidenceSet;
    }


    private void organizeRowsAndIndexes() {


        sortNumericalColumnsMulti(table.getNumericalColumns().values().size());

        buildPredicateIndexes();

        EqualityIndexSizeComparator equalityIndexComparator = new EqualityIndexSizeComparator();

        catPredGroups = new ArrayList<>(predicateSpace.getCategoricalPredicateGroups());
        Collections.sort(catPredGroups, equalityIndexComparator);
        Collections.reverse(catPredGroups);

        numPredGroups = new ArrayList<>(predicateSpace.getNumericalPredicateGroups());
        Collections.sort(numPredGroups, equalityIndexComparator);
        Collections.reverse(numPredGroups);
    }


    private BitSet createNonNullBaseEvidence() {

        BitSet baseEvidence = new BitSet();
        // assuming UNEQ in the evidence, then we correct for EQ
        for (ColumnPredicateGroup catGroup : predicateSpace.getCategoricalPredicateGroups()) {
            IndexedPredicate uneq = catGroup.getUneq();
            baseEvidence.set(uneq.getPredicateId());

        }
        for (NumericalColumnPredicateGroup numGroup : predicateSpace.getNumericalPredicateGroups()) {
            // assuming GT in the evidence, then we correct for LT
            IndexedPredicate uneq = numGroup.getUneq();
            IndexedPredicate gt = numGroup.getGt();
            IndexedPredicate gte = numGroup.getGte();
            baseEvidence.set(uneq.getPredicateId());
            baseEvidence.set(gt.getPredicateId());
            baseEvidence.set(gte.getPredicateId());
        }
        return baseEvidence;

    }

    public void sortNumericalColumnsMulti(int levels) {

        ColumnComparatorByCardinality columnComparator = new ColumnComparatorByCardinality();

        List<NumericalColumn> numCols = new ArrayList<>(table.getNumericalColumns().values());

        if (numCols.isEmpty()) {
            return;
        }

        Collections.sort(numCols, columnComparator);
        Collections.reverse(numCols);

        List<Integer> orderedRowIDs = IntStream.range(0, table.getNUM_RECORDS()).boxed().collect(Collectors.toList());

        if (levels > 0 && levels <= numCols.size())
            numCols = numCols.subList(0, levels);

        RowComparatorMultiColumn rowComparator = new RowComparatorMultiColumn(numCols);
        Collections.sort(orderedRowIDs, rowComparator);

        for (Column c : table.getAllColumns().values()) {

            FloatList orderedValues = new FloatArrayList(table.getNUM_RECORDS());

            for (int tid : orderedRowIDs) {
                orderedValues.add(c.getValueAt(tid));

            }

            c.setValuesList(orderedValues);

        }

    }

    private void buildPredicateIndexes() {

        for (ColumnPredicateGroup catPredicateGroup : predicateSpace.getCategoricalPredicateGroups()) {

            IndexedPredicate eq = catPredicateGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);

            CategoricalColumn catCol = (CategoricalColumn) catPredicateGroup.getColumn();
            if (catCol.containsMissing()) {
                float nullFloatValue = catCol.getFloatDictionary().getFloatMap(CategoricalColumn.DEFAULT_NULL_STRING);
                eqIndex.removeValue(nullFloatValue);
            }

            eq.setEqualityIndex(eqIndex);
        }

        for (NumericalColumnPredicateGroup numPredicateGroup : predicateSpace.getNumericalPredicateGroups()) {

            IndexedPredicate eq = numPredicateGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);


            NumericalColumn numCol = (NumericalColumn) numPredicateGroup.getColumn();
            if (numCol.containsMissing()) {
                float nullFloatValue = Column.DEFAULT_NULL_NUMBER;
                eqIndex.removeValue(nullFloatValue);
            }

            eq.setEqualityIndex(eqIndex);

            if (eqIndex.getIndex().size() >= BinThreshold) {
                numPredicateGroup.setBinnedLT(true);
                LtBinnedIndex ltBinnedIndex = new LtBinnedIndex(eqIndex, table.getNUM_RECORDS());


                numPredicateGroup.getLt().setLtBinnedIndex(ltBinnedIndex);

            } else {

                LtIndex ltIndex = new LtIndex(eqIndex);
                numPredicateGroup.getLt().setLtIndex(ltIndex);

            }

        }

    }
}