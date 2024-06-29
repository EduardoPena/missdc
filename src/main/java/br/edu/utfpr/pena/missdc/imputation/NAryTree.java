package br.edu.utfpr.pena.missdc.imputation;


import br.edu.utfpr.pena.missdc.dc.predicates.space.PredicateSpace;
import br.edu.utfpr.pena.missdc.input.Table;
import br.edu.utfpr.pena.missdc.input.columns.Column;
import org.roaringbitmap.RoaringBitmap;

import java.util.*;


public class NAryTree<T extends Refiner> {

    Table table;
    PredicateSpace predicateSpace;
    Column col;
    RoaringBitmap initialTIDs; // non-null tids for col
    Map<Float, Integer> repairSupportMap;
    Set<RoaringBitmap> violationsSet;
    int currentTuple;
    Set<Integer> currentNullCols;
    private Node<T> root;

    public NAryTree(Table table, PredicateSpace predicateSpace, Column col, Node<T> root) {
        this.table = table;
        this.predicateSpace = predicateSpace;
        this.col = col;
        this.root = root;

        initialTIDs = new RoaringBitmap();
        initialTIDs.add((long) 0, (long) table.getNUM_RECORDS());
        for (Integer tid : col.getTIDsMissing()) {
            initialTIDs.remove(tid);
        }
    }

    public NAryTree(NAryTree<T> othertree) {
        this.table = othertree.table;
        this.predicateSpace = othertree.predicateSpace;
        this.col = othertree.col;

        this.root = othertree.root;
        this.initialTIDs = othertree.initialTIDs.clone();
    }

    public Map<Float, Integer> refine(Integer tid, Set<Integer> nullifiedCols4tid) {

        currentTuple = tid;
        currentNullCols = nullifiedCols4tid;
        repairSupportMap = new HashMap<>();

        for (Node<T> child : root.getChildren()) {
            RoaringBitmap tids = initialTIDs.clone();
            tids.remove(tid);
            refine(child, tids); // pass a copy in the first level
        }
        return repairSupportMap;
    }

    private void refine(Node<T> node, RoaringBitmap tids) {

        if (currentNullCols.contains(node.getData().predicate.getPredicate().getCol1().ColumnIndex)) {
            return;
        }

        node.getData().refine(predicateSpace, tids, currentTuple);// modify tids according to that predicate

        if (tids.isEmpty()) {
            return;
        }

        if (node.isLeaf()) {

            for (int t : tids) {
                float repairCandidate = col.getValueAt(t);
                int oldSupport = repairSupportMap.getOrDefault(repairCandidate, 0);
                repairSupportMap.put(repairCandidate, (oldSupport + 1));
            }


        } else {
            List<Node<T>> nodes = node.getChildren();
            if (nodes.size() > 1) {
                for (Node<T> child : nodes) {
                    refine(child, tids.clone());// makes a copy for further branches
                }
                tids = null;
            } else {
                Node<T> child = nodes.iterator().next();
                refine(child, tids);


            }
        }
    }

    public Set<RoaringBitmap> refine_EQ(Integer tid, Set<Integer> nullifiedCols4tid) {

        currentTuple = tid;
        currentNullCols = nullifiedCols4tid;
        violationsSet = new HashSet<>();

        for (Node<T> child : root.getChildren()) {
            RoaringBitmap tids = initialTIDs.clone();
            tids.remove(tid);
            refine_EQ(child, tids); // pass a copy in the first level
        }
        return violationsSet;
    }

    private void refine_EQ(Node<T> node, RoaringBitmap tids) {

        if (currentNullCols.contains(node.getData().predicate.getPredicate().getCol1().ColumnIndex)) {
            return;
        }

        node.getData().refine(predicateSpace, tids, currentTuple);// modify tids according to that predicate

        if (tids.isEmpty()) {
            return;
        }

        if (node.isLeaf()) {

            violationsSet.add(tids);


        } else {
            List<Node<T>> nodes = node.getChildren();
            if (nodes.size() > 1) {
                for (Node<T> child : nodes) {
                    refine_EQ(child, tids.clone());// makes a copy for further branches
                }
                tids = null;
            } else {
                Node<T> child = nodes.iterator().next();
                refine_EQ(child, tids);


            }
        }
    }

    /**
     * Checks if the tree is empty (root node is null)
     *
     * @return <code>true</code> if the tree is empty,
     * <code>false</code> otherwise.
     */
    public boolean isEmpty() {
        return root == null;
    }

    /**
     * Get the root node of the tree
     *
     * @return the root node.
     */
    public Node<T> getRoot() {
        return root;
    }

    /**
     * Set the root node of the tree. Replaces existing root node.
     *
     * @param root The root node to replace the existing root node with.
     */
    public void setRoot(Node<T> root) {
        this.root = root;
    }

    /**
     * Check if given data is present in the tree.
     *
     * @param key The data to search for
     * @return <code>true</code> if the given key was found in the tree,
     * <code>false</code> otherwise.
     */
    public boolean exists(T key) {
        return find(root, key);
    }

    /**
     * Get the number of nodes (size) in the tree.
     *
     * @return The number of nodes in the tree
     */
    public int size() {
        return getNumberOfDescendants(root) + 1;
    }

    /**
     * Get the number of descendants a given node has.
     *
     * @param node The node whose number of descendants is needed.
     * @return the number of descendants
     */
    public int getNumberOfDescendants(Node<T> node) {
        int n = node.getChildren().size();
        for (Node<T> child : node.getChildren())
            n += getNumberOfDescendants(child);

        return n;

    }

    private boolean find(Node<T> node, T keyNode) {
        boolean res = false;
        if (node.getData().equals(keyNode)) return true;

        else {
            for (Node<T> child : node.getChildren())
                if (find(child, keyNode)) res = true;
        }

        return res;
    }

    private Node<T> findNode(Node<T> node, T keyNode) {
        if (node == null) return null;
        if (node.getData().equals(keyNode)) return node;
        else {
            Node<T> cnode = null;
            for (Node<T> child : node.getChildren())
                if ((cnode = findNode(child, keyNode)) != null) return cnode;
        }
        return null;
    }

    /**
     * Get the list of nodes arranged by the pre-order traversal of the tree.
     *
     * @return The list of nodes in the tree, arranged in the pre-order
     */
    public ArrayList<Node<T>> getPreOrderTraversal() {
        ArrayList<Node<T>> preOrder = new ArrayList<Node<T>>();
        buildPreOrder(root, preOrder);
        return preOrder;
    }

    /**
     * Get the list of nodes arranged by the post-order traversal of the tree.
     *
     * @return The list of nodes in the tree, arranged in the post-order
     */
    public ArrayList<Node<T>> getPostOrderTraversal() {
        ArrayList<Node<T>> postOrder = new ArrayList<Node<T>>();
        buildPostOrder(root, postOrder);
        return postOrder;
    }

    private void buildPreOrder(Node<T> node, ArrayList<Node<T>> preOrder) {
        preOrder.add(node);


        for (Node<T> child : node.getChildren()) {
            buildPreOrder(child, preOrder);
        }
    }

    private void buildPostOrder(Node<T> node, ArrayList<Node<T>> postOrder) {
        for (Node<T> child : node.getChildren()) {
            buildPostOrder(child, postOrder);
        }
        postOrder.add(node);
    }

    /**
     * Get the list of nodes in the longest path from root to any leaf in the tree.
     * <p>
     * For example, for the below tree
     * <pre>
     *          A
     *         / \
     *        B   C
     *           / \
     *          D  E
     *              \
     *              F
     * </pre>
     * <p>
     * The result would be [A, C, E, F]
     *
     * @return The list of nodes in the longest path.
     */
    public ArrayList<Node<T>> getLongestPathFromRootToAnyLeaf() {
        ArrayList<Node<T>> longestPath = null;
        int max = 0;
        for (ArrayList<Node<T>> path : getPathsFromRootToAnyLeaf()) {
            if (path.size() > max) {
                max = path.size();
                longestPath = path;
            }
        }
        return longestPath;
    }

    /**
     * Get the height of the tree (the number of nodes in the longest path from root to a leaf)
     *
     * @return The height of the tree.
     */
    public int getHeight() {
        return getLongestPathFromRootToAnyLeaf().size();
    }

    /**
     * Get a list of all the paths (which is again a list of nodes along a path) from the root node to every leaf.
     *
     * @return List of paths.
     */
    public ArrayList<ArrayList<Node<T>>> getPathsFromRootToAnyLeaf() {
        ArrayList<ArrayList<Node<T>>> paths = new ArrayList<ArrayList<Node<T>>>();
        ArrayList<Node<T>> currentPath = new ArrayList<Node<T>>();
        getPath(root, currentPath, paths);

        return paths;
    }

    private void getPath(Node<T> node, ArrayList<Node<T>> currentPath, ArrayList<ArrayList<Node<T>>> paths) {
        if (currentPath == null) return;

        currentPath.add(node);

        if (node.getChildren().size() == 0) {
            // This is a leaf
            paths.add(clone(currentPath));
        }
        for (Node<T> child : node.getChildren())
            getPath(child, currentPath, paths);

        int index = currentPath.indexOf(node);
        for (int i = index; i < currentPath.size(); i++)
            currentPath.remove(index);
    }

    private ArrayList<Node<T>> clone(ArrayList<Node<T>> list) {
        ArrayList<Node<T>> newList = new ArrayList<Node<T>>();
        for (Node<T> node : list)
            newList.add(new Node<T>(node));

        return newList;
    }

    public enum RefinementMode {
        UNEQ, EQ
    }

}