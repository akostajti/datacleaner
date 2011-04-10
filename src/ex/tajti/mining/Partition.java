package ex.tajti.mining;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Represents a partition. A partition belongs to an attribute set and contains equivalence classes.
 * For every different value set on that attribute set there's an EC in the partition.
 * <br>
 * This implementation accepts only integers as row IDs.
 *
 * @author tajti ákos
 */
public class Partition {

    /**
     * The list of ECs in the partition.
     */
    private List<EquivalenceClass<Object, Integer>> classes;

    /**
     * The attribute set of the partition.
     */
    private String attribute;

    /**
	 * The level of the processing on which the partition was created.
     */
    private int level = -1;

    /**
     * The number of rows removed by the <code>strip()</code> method.
     */
    private int strippedRows;

    /**
     * The list of row IDs removed by <code>strip()</code>.
     */
    private Set<Integer> rowsStripped = new HashSet<Integer>();

    public Partition(String attribute) {
        this.attribute = attribute;
        classes = new ArrayList<EquivalenceClass<Object, Integer>>();
    }

    /**
     * Adds a list of ECs to the partition.
     *
     * @param attribute
     * @param classes
     */
    public Partition(String attribute, List<EquivalenceClass<Object, Integer>> classes) {
        this.attribute = attribute;
        this.classes = new ArrayList<EquivalenceClass<Object, Integer>>(classes);
    }

    /**
     * Computes the level on which the partition was created.
     *
     * @param 
     * @return
     */
    private int getLevelNumber(String attributeList) {
        return attributeList.replaceAll("[^:]", "").length();
    }

    /**
     * Returns the number of ECs in the partition.
     *
     * @return
     */
    public int getNumberOfClasses() {
        return getClasses().size();
    }

    /**
	 * Finds the EC in the partition that has the <code>attributeValue</code> value on the attribute set.
	 * If there's no such EC the return value is <code>null</code>.
	 *
     * @param attributeValue
     * @return
     */
    public EquivalenceClass<Object, Integer> getClassWithClassifier(Object attributeValue) {
        for (EquivalenceClass<Object, Integer> cl : getClasses()) {
            if (cl.getClassifier().equals(attributeValue)) {
                return cl;
            }
        }

        return null;
    }

    /**
	 * Adds a row with <code>rowId</code> to the partition. First it tries to find an EC
	 * for <code>attributeValue</code> and if finds it then adds the row to that. Otherwise
	 * a new EC is creates.
	 *
     * @param rowId
     * @param attributeValue
     */
    public void addRow(Integer rowId, Object attributeValue) { //TODO: review
        EquivalenceClass<Object, Integer> eqClass = getClassWithClassifier(attributeValue);

//        logger.info(attribute + " eqClass for " + attributeValue + ": " + eqClass);
        if (eqClass == null) {
            eqClass = new EquivalenceClass<Object, Integer>(/*getAttribute(),*/attributeValue);
            getClasses().add(eqClass);
        }

        eqClass.addRow(rowId);
    }

    /**
	 * Removes every ECs containing only one rows from the partition. The removed rows
	 * are added to <code>rowsStripped</code>.
     */
    public void strip() { 
        Iterator<EquivalenceClass<Object, Integer>> it = getClasses().iterator();
        while (it.hasNext()) {
            EquivalenceClass<Object, Integer> cl = it.next();
            if (cl.getSize() == 1) {
                ++strippedRows;
                ++stripped;
                rowsStripped.add(cl.getRows().get(0));
                it.remove();
            }
        }
    }

    /**
	 * Returns thr attribute set of the partition.
     *
     * @return the attribute
     */
    public String getAttribute() {
        return attribute;
    }

    /**
     * Adds all ECs from the set to the partition.
	 *
     * @param set
     */
    public void addClasses(Set<EquivalenceClass<Object, Integer>> set) {
        for (Iterator<EquivalenceClass<Object, Integer>> it = set.iterator(); it.hasNext();) {
            getClasses().add(it.next());
        }
    }

    /**
	 * Multiplies this partition with <code>part</code>. The result is computed as follows:
	 * <br/>
	 * If this partition is generated based on attribute set <code>X</code> and <code>part</code>
	 * is based on <code>A</code> then the result partition is based on <code>XA</code> (concatenation
	 * of the attribute sets).
     *
     * @param part
     * @return null if there was an error
     */
    public Partition multiply(Partition part) {
        if (part == null) {
            return null;
        }

        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        int size = classes.size();
        for (int i = 0; i < size; i++) {
            EquivalenceClass<Object, Integer> cl = classes.get(i);

            for (Integer rowId : cl.getRows()) {
                map.put(rowId, i);
            }
        }


        Partition result = new Partition(attribute + ":" + part.attribute);

        List<EquivalenceClass<Object, Integer>> otherClasses = part.getClasses();
        for (EquivalenceClass<Object, Integer> cl : otherClasses) {
            Map<Integer, EquivalenceClass<Object, Integer>> eqClasses = new HashMap<Integer, EquivalenceClass<Object, Integer>>();
            for (Integer rowId : cl.getRows()) {
                Integer o = map.get(rowId);
                if (o == null) { 
                    continue;
                }

                if (eqClasses.get(o) != null) { 
                    eqClasses.get(o).addRow(rowId);
                } else {
                    EquivalenceClass<Object, Integer> clazz = new EquivalenceClass<Object, Integer>(attribute + ":" + part.attribute);
                    clazz.addRow(rowId);
                    eqClasses.put(o, clazz);
                }
            }

            List<EquivalenceClass<Object, Integer>> resultClasses = result.getClasses();
            for (Map.Entry<Integer, EquivalenceClass<Object, Integer>> entry : eqClasses.entrySet()) {
                resultClasses.add(entry.getValue());
            }
        }

        result.strip();
        return result;
    }

    /**
     * Returns the level on which the partition was created.
     *
     * @return the level
     */
    public int getLevel() {
        if (level == -1) {
            level = getLevelNumber(attribute);
        }
        return level;
    }

    public static long iterations = 0;
    public static long continued = 0;
    public static long stripped = 0;
    //private static List<EquivalenceClass<Object, Integer>> result;

    /**
	 * Returns the list of equivalence classes that must be deleted from <code>extended</code>
	 * in order to make the <code>attribute -> extended.attribute</code> functional dependency
	 * valid. <code>attribute</code> must be a subset of <code>extended.attribute</code>. If this
	 * is not the case or <code>extended</code> is <code>null</code> the method returns <code>null</code>.
     *
     * @param extended
     * @return
     */
    public Collection<Integer> getRowsToDelete(Partition extended) { //TODO: test this
        if (extended == null /*|| !extended.attribute.contains(attribute)*/) { 
            System.out.println("unsupported attribute");
            return null;
        }
        Set<Integer> result = new HashSet<Integer>();


        List<EquivalenceClass<Object, Integer>> extendedClasses = extended.classes;

        Map<Integer, Integer> idAndSize = new HashMap<Integer, Integer>(extendedClasses.size());
        for (EquivalenceClass<Object, Integer> cl : extendedClasses) {
            idAndSize.put(cl.getRandomRowId(), cl.getSize());
            ++iterations;
        }

        for (EquivalenceClass<Object, Integer> cl : classes) {
            int max = 0; // the size of the biggest subset
            Integer maxRow = null;
            List<Integer> clRows = cl.getRows();
            for (Integer row : clRows) { // TODO: is SortedSet better?
                Integer size = idAndSize.get(row);
                if (size != null && size > max) {
                    max = size;
                    maxRow = row;
                }
                ++iterations;
            }

            EquivalenceClass<Object, Integer> maxSuperclass = null;
            if (max == 0) { // the case when all ECs were stripped. one must be put back.
                EquivalenceClass<Object, Integer> newClass = new EquivalenceClass<Object, Integer>();
                newClass.addRow(clRows.get(0));
                extended.classes.add(newClass);
                continue; 
            }

            for (EquivalenceClass<Object, Integer> clazz : extendedClasses) {

                if (clazz.rows.contains(maxRow)) {
                    maxSuperclass = clazz;
                    break;
                }
                ++iterations;
            }
            Collection<Integer> maxRows = maxSuperclass.rows;
            for (Integer row : clRows) {
                ++iterations;
                if (!maxRows.contains(row)) {

                    for (EquivalenceClass<Object, Integer> clazz : extendedClasses) {
                        if (clazz.rows.contains(row)) {

                            result.addAll(clazz.getRows());
                            ++iterations;
                            break;
                        }
                    }
                }
            }
        }

        if (strippedRows != 0 || extended.strippedRows != 0) {
            List<Integer> extendedRowsStripped = new ArrayList<Integer>(extended.rowsStripped);
            extendedRowsStripped.removeAll(rowsStripped);
            result.addAll(extendedRowsStripped);
        }

        return result;
    }

    /**
	 * Sums the number of rows of the ECs in <code>clazzes</code> and returns the result. 
     *
     * @param clazzes 
     * @return 
     */
    public static Integer rowCount(List<EquivalenceClass<Object, Integer>> clazzes) {
        Integer count = 0;
        for (EquivalenceClass<Object, Integer> cl : clazzes) {
            count += cl.getSize();
        }

        return count;
    }

    /**
	 * Creates the union of this partition ant <code>other</code>. The two partition must be based on 
	 * the same attribute set. The method modifies both objects.
     *
     * @param other
     * @return
     */
    public Partition union(Partition other) {
        Map<Object, Integer> classIndexForClassifier = new HashMap<Object, Integer>();
        for (int i = 0; i < classes.size(); i++) {
            classIndexForClassifier.put(classes.get(i).getClassifier(), i);
        }

        List<EquivalenceClass<Object, Integer>> newClasses = new ArrayList<EquivalenceClass<Object, Integer>>(classes);
        for (EquivalenceClass<Object, Integer> clazz : other.classes) {
            Integer classIndex = classIndexForClassifier.get(clazz.getClassifier());
            if (classIndex != null) {
                newClasses.get(classIndex).addRows(clazz.getRows());
            } else {
                newClasses.add(clazz);
            }
        }

        Partition p = new Partition(attribute, newClasses);
        return p;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[partition ").append(getAttribute()).append(": ");
        for (EquivalenceClass<Object, Integer> cl : getClasses()) {
            builder.append(cl.toString());
        }

        builder.append("]");
        return builder.toString();
    }

    /**
     * Returns the ECs of the partition.
     *
     * @return the classes
     */
    public List<EquivalenceClass<Object, Integer>> getClasses() {
        return classes;
    }

    /**
     * Removes all ECs from the partition.
     */
    public void clear() {
        classes.clear();
    }

    /**
     * Returns the number of rows removed by <code>strip()</code>.
     *
     * @return the strippedRows
     */
    public int getStrippedRows() {
        return strippedRows;
    }

    /**
     * Returns the list of rows removed by <code>strip()</code>.
     *
     * @return the rowsStripped
     */
    public Set<Integer> getRowsStripped() {
        return rowsStripped;
    }
}
