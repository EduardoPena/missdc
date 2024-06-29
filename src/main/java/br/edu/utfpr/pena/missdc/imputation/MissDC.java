package br.edu.utfpr.pena.missdc.imputation;


import br.edu.utfpr.pena.missdc.dc.predicates.IndexedPredicate;
import br.edu.utfpr.pena.missdc.dc.predicates.groups.ColumnPredicateGroup;
import br.edu.utfpr.pena.missdc.dc.predicates.groups.NumericalColumnPredicateGroup;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.EqualityIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.LtBinnedIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.indexes.LtIndex;
import br.edu.utfpr.pena.missdc.dc.predicates.space.PredicateSpace;
import br.edu.utfpr.pena.missdc.dc.predicates.space.builder.PredicateSpaceBuilder;
import br.edu.utfpr.pena.missdc.discovery.DCDiscoverer;
import br.edu.utfpr.pena.missdc.discovery.evidence.EvidenceSet;
import br.edu.utfpr.pena.missdc.imputation.ml.RFImputer;
import br.edu.utfpr.pena.missdc.input.Table;
import br.edu.utfpr.pena.missdc.input.columns.CategoricalColumn;
import br.edu.utfpr.pena.missdc.input.columns.Column;
import br.edu.utfpr.pena.missdc.input.reader.CSVInput;
import br.edu.utfpr.pena.missdc.utils.bitset.BitUtils;
import it.unimi.dsi.fastutil.floats.Float2ObjectMap;
import it.unimi.dsi.fastutil.floats.FloatList;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MissDC {

    private static final int BinThreshold = 2000;
    final private static Logger log = LoggerFactory.getLogger(MissDC.class);

    final private String dirtyDatasetPath;
    private final boolean applyValuesAfterImputation = true;

    private final boolean saveImputed = true;
    private final double ImputationRatioThreshold = 0.2;
    String[][] imputedDataset;
    DCDiscoverer dcDiscoverer;
    private PredicateSpace predicateSpace;
    private Map<Integer, Set<Integer>> tid2NullColMap;
    private Map<BitSet, BitSet> dc2Cols;
    private Map<BitSet, ArrayList<Integer>> dcs2ExecutionOrder;
    private Map<RepairLocation, Float> repairs;
    private final boolean useApproximateDCs;
    private final boolean useML;

    public MissDC(String dirtyDatasetPath) {
      this(dirtyDatasetPath, false, false);
    }

    public MissDC(String dirtyDatasetPath, boolean useApproximateDCs, boolean useML) {
        this.dirtyDatasetPath = dirtyDatasetPath;
        this.useApproximateDCs = useApproximateDCs;
        this.useML = useML;
    }

    public void run() throws Exception {

        log.info("Running MissDC ...");

        Set<BitSet> dcs = discoverDCs(useApproximateDCs);

        long startTime = System.nanoTime();

        CSVInput dirtyInput = new CSVInput(dirtyDatasetPath);
        Table dirtyTable = dirtyInput.getTable();
        RoaringBitmap allTIDs = new RoaringBitmap();
        allTIDs.add(0, (long) dirtyTable.getNUM_RECORDS());

        if (saveImputed) {
            imputedDataset = initializeDataset(dirtyTable);
        }

        predicateSpace = new PredicateSpaceBuilder().build(dirtyTable);

        List<Column> colsWithNulls = dirtyTable.getColsWithNulls();

        buildTID2NullColMap(colsWithNulls);
        buildCols4Dcs(dcs, predicateSpace);
        buildPredicateOrder4Dcs(dcs, predicateSpace);
        buildPredicateIndexes(predicateSpace, dirtyTable);

        repairs = new ConcurrentHashMap<>();

        colsWithNulls.sort(Comparator.comparingLong(Column::getCardinality));

        Set<Column> nonImputedCols = new HashSet<>();


        for (Column col : colsWithNulls) {

            int numMissingInitial = col.getTIDsMissing().size();

            NAryTree<Refiner> refinementTree = buildRefinementTree(dirtyTable, dcs, col, false);

            int cores = Runtime.getRuntime().availableProcessors();

            ExecutorService service = Executors.newFixedThreadPool(cores);

            Map<Integer, Float> repairs4col = new ConcurrentHashMap<>();

            if (col.getDomain().size() == 1) {
                Float repair = col.getDomain().iterator().next();
                for (Integer tid : col.getTIDsMissing()) {
                    repairs.put(new RepairLocation(tid, col.ColumnIndex), repair);
                    repairs4col.put(tid, repair);
                }

            } else {


                for (Integer tid : col.getTIDsMissing()) {
                    service.submit(new Runnable() {
                        public void run() {

                            NAryTree<Refiner> refinementTreePar = new NAryTree<Refiner>(refinementTree);
                            Set<Integer> nullifiedCols4tid = tid2NullColMap.get(tid);
                            Map<Float, Integer> repairSupportMap = refinementTreePar.refine(tid, nullifiedCols4tid);

                            if (!repairSupportMap.isEmpty()) { // did not find any repair, continue for next tuple
                                Map.Entry<Float, Integer> entry = repairSupportMap.entrySet().stream().max(Map.Entry.comparingByValue()).get();
                                repairs.put(new RepairLocation(tid, col.ColumnIndex), entry.getKey());
                                repairs4col.put(tid, entry.getKey());
                            }
                        }
                    });
                }
                //wait until all thread finishes
                service.shutdown();
                try {
                    service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    log.error("Error while waiting for threads to finish: " + e.getMessage());
                }
            }

            double remainingMissingRatio = (double) (numMissingInitial - repairs4col.size()) / numMissingInitial;

            if(remainingMissingRatio >= 0.2 && col.getDomain().size()<=1000) {
                nonImputedCols.add(col);
            }

            if (applyValuesAfterImputation) applyImputations(dirtyTable, col, repairs4col);
        }

        if (useML) {
            log.info("Applying ML imputation ...");
            RFImputer columnRFImputer = new RFImputer(dirtyTable, imputedDataset, nonImputedCols);
            columnRFImputer.imputeMissing();
        }

        long endTime = System.nanoTime();
        long inputationTime = (endTime - startTime) / 1000000;
        log.info("Inputation Time: " + inputationTime + " ms");

        if (saveImputed) {
            log.info("Saving imputed dataset ...");
            dirtyInput.saveImputed(imputedDataset);
        }
    }

    private void applyImputations(Table dirtyTable, Column col, Map<Integer, Float> repairs4col) {
        FloatList values = dirtyTable.getColumnByName(col.ColumnName).getValuesList();

        for (Map.Entry<Integer, Float> entry : repairs4col.entrySet()) {
            int tid = entry.getKey();
            float repair = entry.getValue();
            values.set(tid, repair);

            if (saveImputed) {
                if (col instanceof CategoricalColumn catCol) {

                    Float2ObjectMap<String> reverseMapDirty = catCol.buildReverseDictionaryMap();
                    String strRepair = reverseMapDirty.get(repair);
                    imputedDataset[tid][col.ColumnIndex] = strRepair;

                } else {// numerical col
                    imputedDataset[tid][col.ColumnIndex] = "" + repair;
                }
            }

        }
        col.rebuildTIDsMissing(); // to rebuild the TIDs missing after the imputation

        ColumnPredicateGroup group = predicateSpace.getColumn2GroupMap().get(col);

        if (group instanceof NumericalColumnPredicateGroup numGroup) {

            IndexedPredicate eq = numGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);
            eq.setEqualityIndex(eqIndex);
            if (eqIndex.getIndex().size() >= BinThreshold) {
                numGroup.setBinnedLT(true);
                LtBinnedIndex ltBinnedIndex = new LtBinnedIndex(eqIndex, dirtyTable.getNUM_RECORDS());
                numGroup.getLt().setLtBinnedIndex(ltBinnedIndex);
            } else {
                LtIndex ltIndex = new LtIndex(eqIndex);
                numGroup.getLt().setLtIndex(ltIndex);
            }

        } else {
            IndexedPredicate eq = group.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);
            eq.setEqualityIndex(eqIndex);

        }


    }

    private NAryTree<Refiner> buildRefinementTree(Table dirtyTable, Set<BitSet> dcs, Column col, boolean approx) {

        int uneqid = predicateSpace.getColumn2GroupMap().get(col).getUneq().getPredicateId();
        Set<BitSet> dcs4col = BitUtils.getSubSetWithId(dcs, uneqid);

        List<ArrayList<Integer>> dcsPredOrder4col = new ArrayList<>();

        dcs4col.forEach(dc -> {
            ArrayList<Integer> dcWithoutUneq = new ArrayList<>(dcs2ExecutionOrder.get(dc));
            dcWithoutUneq.remove((Integer) uneqid);
            dcsPredOrder4col.add(dcWithoutUneq);
        });

        Node<Refiner> root = new Node<Refiner>(new Refiner(null));
        NAryTree<Refiner> refinementTree = new NAryTree<Refiner>(dirtyTable, predicateSpace, col, root);

        for (ArrayList<Integer> dc : dcsPredOrder4col) {
            addDC(refinementTree, dc);
        }

        return refinementTree;
    }

    private Set<BitSet> filterDCs(EvidenceSet evidenceSet, Set<BitSet> dcs4col, int uneqid) {

        Set<BitSet> filteredDCs = new HashSet<>();

        if (dcs4col.size() == 0) return dcs4col;

        return dcs4col;

    }

    private NAryTree<Refiner> buildRefinementTree_EQ(Table dirtyTable, Set<BitSet> dcs, Column col) {

        int eqid = predicateSpace.getColumn2GroupMap().get(col).getEq().getPredicateId();
        Set<BitSet> dcs4col = BitUtils.getSubSetWithId(dcs, eqid); // get the DCs that contain an UNEQ on this column

        List<ArrayList<Integer>> dcsPredOrder4col = new ArrayList<>();
        dcs4col.forEach(dc -> {
            ArrayList<Integer> dcWithoutEQ = new ArrayList<>(dcs2ExecutionOrder.get(dc));
            dcWithoutEQ.remove((Integer) eqid);
            dcsPredOrder4col.add(dcWithoutEQ);
        });

        Node<Refiner> root = new Node<Refiner>(new Refiner(null));
        NAryTree<Refiner> refinementTree = new NAryTree<Refiner>(dirtyTable, predicateSpace, col, root);

        for (ArrayList<Integer> dc : dcsPredOrder4col) {
            addDC(refinementTree, dc);
        }
        return refinementTree;
    }

    private void addDC(NAryTree<Refiner> tree, ArrayList<Integer> dc) {
        Node<Refiner> current = tree.getRoot();
        for (int pid : dc) {
            IndexedPredicate predicate = predicateSpace.getPredicateById(pid);
            Node<Refiner> stage = new Node<Refiner>(new Refiner(predicate));

            int idx = current.getChildren().indexOf(stage);
            if (idx == -1) {
                current.addChild(stage);
            } else {
                stage = current.getChildAt(idx);
            }

            current = stage;
        }
    }



    private Set<BitSet> discoverDCs(boolean useApproximateDCs) throws Exception {
        dcDiscoverer = new DCDiscoverer(dirtyDatasetPath, useApproximateDCs);
        Set<BitSet> dcs = dcDiscoverer.run();
        return dcs;
    }


    private String[][] initializeDataset(Table dirtyTable) {

        String[][] imputedDataset = new String[dirtyTable.getNUM_RECORDS()][dirtyTable.NUM_ORIGINAL_COLLUMNS];

        for (int rowid = 0; rowid < dirtyTable.getNUM_RECORDS(); rowid++) {
            for (int colid = 0; colid < dirtyTable.NUM_ORIGINAL_COLLUMNS; colid++) {

                Column col = dirtyTable.getAllColumns().get(colid);
                if (col instanceof CategoricalColumn catCol) {

                    Float2ObjectMap<String> reverseMapDirty = catCol.buildReverseDictionaryMap();

                    float value = col.getValueAt(rowid);
                    String svalue = reverseMapDirty.get(value);

                    if (svalue.equals(Column.DEFAULT_NULL_STRING)) {
                        imputedDataset[rowid][colid] = null;
                    } else {
                        imputedDataset[rowid][colid] = svalue;
                    }

                } else {

                    float value = col.getValueAt(rowid);

                    if (Float.compare(value, Column.DEFAULT_NULL_NUMBER) == 0) {
                        imputedDataset[rowid][colid] = null;
                    } else {
                        imputedDataset[rowid][colid] = "" + value;
                    }

                }


            }
        }

        return imputedDataset;
    }


    private void buildTID2NullColMap(List<Column> colsWithNulls) {
        tid2NullColMap = new LinkedHashMap<>();
        for (Column col : colsWithNulls) {
            for (Integer tid : col.getTIDsMissing()) {
                tid2NullColMap.putIfAbsent(tid, new HashSet());
                tid2NullColMap.get(tid).add(col.ColumnIndex);
            }
        }
    }


    private void buildCols4Dcs(Set<BitSet> dcs, PredicateSpace predicateSpace) {
        dc2Cols = new HashMap<>();
        for (BitSet dc : dcs) {
            BitSet dcCols = new BitSet();
            for (int i = dc.nextSetBit(0); i >= 0; i = dc.nextSetBit(i + 1)) {
                int colid = predicateSpace.getPredicateById(i).getPredicate().getCol1().ColumnIndex;
                dcCols.set(colid);
            }
            dc2Cols.put(dc, dcCols);
        }
    }

    private void buildPredicateOrder4Dcs(Set<BitSet> dcs, PredicateSpace predicateSpace) {

        dcs2ExecutionOrder = new HashMap<>();

        for (BitSet dc : dcs) {

            List<Integer> eqs = new ArrayList<>();
            List<Integer> uneqs = new ArrayList<>();
            List<Integer> ranges = new ArrayList<>();

            for (int i = dc.nextSetBit(0); i >= 0; i = dc.nextSetBit(i + 1)) {
                IndexedPredicate pred = predicateSpace.getPredicateById(i);
                if (pred.isEQ()) {
                    eqs.add(i);
                } else if (pred.isUNEQ()) {
                    uneqs.add(i);
                } else {
                    ranges.add(i);
                }
            }

            eqs.sort(Comparator.comparingLong(o -> predicateSpace.getPredicateById(o).getPredicate().getCol1().getCardinality()));
            Collections.reverse(eqs);

            ranges.sort(Comparator.comparingLong(o -> predicateSpace.getPredicateById(o).getPredicate().getCol1().getCardinality()));
            Collections.reverse(ranges);

            uneqs.sort(Comparator.comparingLong(o -> predicateSpace.getPredicateById(o).getPredicate().getCol1().getCardinality()));
            Collections.reverse(uneqs);

            ArrayList<Integer> executionOrder = new ArrayList<>();

            executionOrder.addAll(eqs);
            executionOrder.addAll(ranges);
            executionOrder.addAll(uneqs);

            dcs2ExecutionOrder.put(dc, executionOrder);
        }
    }

    private void buildPredicateIndexes(PredicateSpace predicateSpace, Table table) {

        for (ColumnPredicateGroup catPredicateGroup : predicateSpace.getCategoricalPredicateGroups()) {
            IndexedPredicate eq = catPredicateGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);
            eq.setEqualityIndex(eqIndex);
        }

        for (NumericalColumnPredicateGroup numPredicateGroup : predicateSpace.getNumericalPredicateGroups()) {
            IndexedPredicate eq = numPredicateGroup.getEq();
            EqualityIndex eqIndex = new EqualityIndex(eq);
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

    public class RepairLocation {
        protected final int tid;
        protected final int colID;

        public RepairLocation(int tid, int colID) {
            this.tid = tid;
            this.colID = colID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RepairLocation that = (RepairLocation) o;
            return tid == that.tid && colID == that.colID;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tid, colID);
        }
    }


}