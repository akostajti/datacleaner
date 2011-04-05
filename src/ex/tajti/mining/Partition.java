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
     * A partíció ekvivalencia osztályait tároló lista.
     */
    private List<EquivalenceClass<Object, Integer>> classes;
    /**
     * Az attribútum neve, amihez a partíció tartozik.
     */
    private String attribute;
    /**
     * A feldolgozás szintje, amin a partíciót létrehozta az alkalmazás. Optimalizácisó
     * szerepe van: ez alapján dönthető el, hogy szükség van-e a partícióra, vagy sem.
     */
    private int level = -1;
    /**
     * A <code>strip()</code> metódus által eltávolított sorok száma.
     */
    private int strippedRows;
    /**
     * A <code>strip()</code> metódus által eltávolított sorazonosítók halmaza.
     */
    private Set<Integer> rowsStripped = new HashSet<Integer>();

    /**
     * Létrehoz egy példányt <code>attribute</code> atribútumhoz.
     *
     * @param attribute
     */
    public Partition(String attribute) {
        this.attribute = attribute;
        classes = new ArrayList<EquivalenceClass<Object, Integer>>();
    }

    /**
     * Létrehoz egy partíciót a <code>attribute</code> attribútumhoz és hozzáadja
     * <codde>classes</code> ekvivalencia osztályait.
     *
     * @param attribute
     * @param classes
     */
    public Partition(String attribute, List<EquivalenceClass<Object, Integer>> classes) {
        this.attribute = attribute;
        this.classes = new ArrayList<EquivalenceClass<Object, Integer>>(classes);
    }

    /**
     * Az attribútum halmaz alapján kiszámítja, hogy hányadik szinten jött létre
     * a partícó.
     *
     * @param attributeList Az attribútumok :-tal elválasztott listája. Egy attribútum
     * esetn nincs :.
     * @return
     */
    private int getLevelNumber(String attributeList) {
        return attributeList.replaceAll("[^:]", "").length();
    }

    /**
     * Visszaadja a partíció ekvivalencia osztályainak számát.
     *
     * @return
     */
    public int getNumberOfClasses() {
        return getClasses().size();
    }

    /**
     * Visszadja azt az ekvivalencia osztályt, amihez a <code>attributeValue</code>
     * classifier érték tartozik. Ha nics ilyen, akkor a viszatérési érték <code>null</code>.
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
     * Hozzáad egy sort a partícióhoz. A sor azonosítója <code>rowId</code> és a sor
     * értéke a partícióhoz tartozó attribútum halmazon <code>attributeValue</code>.
     *
     * @param rowId
     * @param attributeValue
     */
    public void addRow(Integer rowId, Object attributeValue) { //TODO: még ezt a metódus megnézni
        EquivalenceClass<Object, Integer> eqClass = getClassWithClassifier(attributeValue);

//        logger.info(attribute + " eqClass for " + attributeValue + ": " + eqClass);
        if (eqClass == null) {
            eqClass = new EquivalenceClass<Object, Integer>(/*getAttribute(),*/attributeValue);
            getClasses().add(eqClass);
        }

        eqClass.addRow(rowId);
    }

    /**
     * Eltávolít minden egyelemű ekvivalencia osztályt a partícióból, miközben a
     * <code>rowsStripped</code> halmazhoz adja az eltávolított azonosítókat.
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
     * Visszaadja azt az attribútum halmazt, amihez a partíció tartozik.
     *
     * @return the attribute
     */
    public String getAttribute() {
        return attribute;
    }

    /**
     * Egy ekvivalencia osztály halmaz minden elemét hozzáadja a partícióhoz.
     *
     * @param set
     */
    public void addClasses(Set<EquivalenceClass<Object, Integer>> set) {
        for (Iterator<EquivalenceClass<Object, Integer>> it = set.iterator(); it.hasNext();) {
            getClasses().add(it.next());
        }
    }

    /**
     * Az aktuális partíciót összeszorozza a <code>part</code> partícióval.
     * <br>
     * Ha az adott partíció az <code>X</code> attribtum halmazhoz tartozik <code>part</code>
     * pedig <code>A</code>-hoz, akkor a szorzás eredménye az <code>XA</code>
     * attribútum halmazhoz tartozó partícdió.
     *
     * @param part
     * @return Az előállított partíció, vagy <code>null</code>, ha hiba történt.
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
     * Visszadja annak a szintnek a számát, amin a partíciót létrehozta az algoritmus.
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
     * Visszaadja azoknak az ekvivalencia osztályoknak a listáját, amit az <code>extended</code>
     * partícióból törölni kell ahhoz, hogy az <code>attribute -> extended.attribute</code>
     * függőség teljesüljön. Itt <code>attribute</code> része <code>extended.attribute</code>-nak.
     * Ha ez nem teljesül, vagy <code>extended == null</code>, akkor a metódus
     * <code>null</code>-t ad vissza.
     *
     * @param extended
     * @return
     */
    public Collection<Integer> getRowsToDelete(Partition extended) { //TODO: jó alaposan tesztelni és optimalizálni
        if (extended == null /*|| !extended.attribute.contains(attribute)*/) { //TODO: ezt a fdeltételt jóhszemből vettem ki.
            System.out.println("unsupported attribute");
            return null;
        }
        //TODO: itt eredetileg arraylist állt, nemtom, az gyorsabb-e
        Set<Integer> result = new HashSet<Integer>();


        List<EquivalenceClass<Object, Integer>> extendedClasses = extended.classes;

        Map<Integer, Integer> idAndSize = new HashMap<Integer, Integer>(extendedClasses.size());
        for (EquivalenceClass<Object, Integer> cl : extendedClasses) {
            idAndSize.put(cl.getRandomRowId(), cl.getSize());
            ++iterations;
        }

        for (EquivalenceClass<Object, Integer> cl : classes) {
            int max = 0; //a maximális méretű részhalmaz
            Integer maxRow = null;
            List<Integer> clRows = cl.getRows();
            for (Integer row : clRows) {//TODO: lehetne SortedSetet használni, akkor ez sokkal egyszerűbb lenne
                Integer size = idAndSize.get(row);
                if (size != null && size > max) {
                    max = size;
                    maxRow = row;
                }
                ++iterations;
            }

            EquivalenceClass<Object, Integer> maxSuperclass = null;
            if (max == 0) { //ez akkor lehet, ha minden részhalmazát törölte a strip, ilyenkor az egyiket visza kell rakni
                EquivalenceClass<Object, Integer> newClass = new EquivalenceClass<Object, Integer>();
                newClass.addRow(clRows.get(0));
                extended.classes.add(newClass);
                continue; //TODO: ez nemtom kell-e ilyenkor
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
     * Megszámolja, hány sor van összesen ekvivalencia osztályok egy listájában.
     *
     * @param clazzes Ekvivalencia osztályokat tartalmazó lista.
     * @return Az ekvivalencia osztáylok sorainak száma összesítve.
     */
    public static Integer rowCount(List<EquivalenceClass<Object, Integer>> clazzes) {
        Integer count = 0;
        for (EquivalenceClass<Object, Integer> cl : clazzes) {
            count += cl.getSize();
        }

        return count;
    }

    /**
     * Elkészíti a partíció únióját <code>partition</code> partícióval. A két partíció 
     * ugyanazon az attribútumon van értelmezve, de más sorok alapján készült.
     * Az aktuális partíció (amire a metódust meghívtuk) is módosul.
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
     * Visszaadja a partíció ekvivalencia osztályainak listáját.
     *
     * @return the classes
     */
    public List<EquivalenceClass<Object, Integer>> getClasses() {
        return classes;
    }

    /**
     * Kiüríti az ekvivalencia osztályok listáját.
     */
    public void clear() {
        classes.clear();
    }

    /**
     * Visszaadja a <code>strip()</code> metódus által eltávolított sorok számát.
     *
     * @return the strippedRows
     */
    public int getStrippedRows() {
        return strippedRows;
    }

    /**
     * Visszaadja a <code>strip()</code> metódus által eltávolított sorok listáját.
     *
     * @return the rowsStripped
     */
    public Set<Integer> getRowsStripped() {
        return rowsStripped;
    }
}
