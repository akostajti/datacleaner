package ex.tajti.mining;

import com.mysql.jdbc.ResultSetMetaData;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A módosított TANE algoritmust implementáló osztály.
 *
 * @author tajti ákos
 */
public class Cleaner {

    static SimpleDateFormat format = new SimpleDateFormat("yMd-Hm");
    /**
     * Annak a táblának a neve, amin az algoritmust végre kell hajtani.
     */
    private String table;
    /**
     * A JDBC url, amivel a metóddusok csatlakozhatnak az adatbázishoz.
     */
    private String jdbcUrl;
    /**
     * Azoknak az attribútumoknak a listája, amiket az algoritmusnak figyelmbe kell
     * vennie. A kulcsokat ki kell hagyni ezek közül.
     */
    private List<String> attributes;
    /**
     * Azoknak a függőségeknek a listája, amelyek érvényesek. Egy függőség formátuma:
     * att1:att2->att3
     */
    private List<String> dependencies;
    /**
     * A jelöltlistákat tartalmazó map. A kulcs mindig egy atribútum halmaz, a hozzá
     * tartozó érték pedig az arra az attribútum halmazra vonatkozó jelöltek listája.
     */
    private Map<String, List<String>> candidateLists;
    /**
     * A partíciókat tartalmazó <code>Map</code>. Ebből a mapből törlődnek azok a
     * partíciók, amelyekre már nincs szükség.
     */
    private Map<String, Partition> partitions;
    /**
     * A küszöbérték. Ha a törlendő sorok aránya az összes sorhoz képest ennél kisebb,
     * akor a függőség érvényes.
     */
    private double epsilon = 0.05;
    /**
     * Az az érték, aminél kisebb hibával szeretnénk dolgozni, ha mintát veszünk.
     */
    private double delta = 0.05;
    /**
     * Ha true, akkor mintavételezéssel megyünk, ha false, akkor nem.
     */
    private boolean sampled;
    /**
     * Ha igaz, aor darabokban dolgozza fel a tábla sorait, ha hamis, akkor egyben.
     */
    private boolean chunks;
    /**
     * Megmondja, hogy mekkora darabokban kell feldolgozni a srokat.
     */
    private int chunkSize;
    /**
     * A sorok száma a lekérdezés eredményében.
     */
    private int numberOfRows;
    /**
     * Ezzel logolok.
     */
    private Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    /**
     * Az egyes függőségekhez tartozó törlendő sorokat tartalmazó map.
     */
    private Map<String, Collection<Integer>> deletandMap = new HashMap<String, Collection<Integer>>();
    /**
     * Az ellenőrzött függőségek száma.
     */
    public static int dependenciesChecked = 0;
    /**
     * A lehetséges függőségek száma (<code>possibleDependencies >= checkedDependencies</code>).
     */
    public static int possibleDependencies = 0;
    /**
     * A kiszámított mintaméret.
     */
    private int sampleSize;
    /**
     * A használt JDBC ddriver neve.
     */
    private String jdbcDriver;

    /**
     * Eltávolítja azokat a partíciókat, amelyekre <code>levelNumber</code> szinten
     * már nincs szükség.
     *
     * @param levelNumber
     */
    private void cleanPartitions(int levelNumber) {
        for (Iterator<Entry<String, Partition>> it = partitions.entrySet().iterator(); it.hasNext();) {
            Entry<String, Partition> entry = it.next();

            String key = entry.getKey();
            //TODO: emiatt is lehet nullpointerexception
            int levelOfPartition = entry.getValue().getLevel();
            if (levelOfPartition != 0 && levelOfPartition + 1 < levelNumber - 1) {
                logger.info("cleaning up partition for " + key);
                it.remove();
            }
        }
    }

    /**
     * Rendezi az alap attribútumokhoz tartozó partíciókat.
     */
    private void sortBasePartitions() {
        Collections.sort(attributes, new Comparator<String>() {

            public int compare(String o1, String o2) {
                Partition part1 = partitions.get(o1);
                Partition part2 = partitions.get(o2);

                //ekvivalenciaosztályok száma szerint csökkenőleg rendezi a partíciókat
                return part1.getNumberOfClasses() - part2.getNumberOfClasses();
            }
        });
    }

    /**
     * Egy állományba írja az eredményeket. Az állomány a reports könyvtárba kerül. Ha
     * a könyvtár nem létezik, akkor a metódus létrehozza.
     * 
     * @param builder
     */
    private static void serializeResults(StringBuilder builder) {
        FileOutputStream out = null;
        File f = new File("reports");
        if (!f.exists()) {
            f.mkdir();
        }
        try {

            String file = "reports/report-" + format.format(new Date()) + ".report";
            out = new FileOutputStream(file);
            out.write(builder.toString().getBytes());
        } catch (IOException ex) {
            Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ex) {
                    Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    //<editor-fold desc="a legfontosabb metódusok, az algo lényege">
    /**
     * Felépíti a lekérdezést az attribútumlista és a táblanév alapján. Figyelembe veszi,
     * hogy használunk-e mintát és hogy a partíciókat inkrementálisan kell-e előállítani.
     * Az inkrementális előállítás jelenleg csak MySQL esetén működik.
     *
     * @return A lekérdezés.
     */
    private String createQuery() {
        //ha mintázott, akkor kiszámítja a mintaméretet.
        if (sampled) {
            sampleSize = (int) ((Math.sqrt((double) numberOfRows) / epsilon) * (attributes.size() + Math.log(1 / delta)));
        }
        if (attributes == null || attributes.size() == 0) {
            return null;
        }
        StringBuilder builder = new StringBuilder("select ");
        for (String attribute : attributes) {
            builder.append(attribute).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);
        builder.append(" from ").append(table);

        //TODO: ennél kifinomultabb mintakezelés kellene
        if (!chunks && sampled && sampleSize < numberOfRows) {
            //builder.append(" where idje mod " + Math.ceil((double)numberOfRows / sampleSize) + " = 0");
            builder.append(" limit 1," + sampleSize);
        }
        return builder.toString();
    }

    /**
     * A lekérdezést úgy építi fel, hogy az csak a <code>firstRow</code>-adik sortól
     * kezdődő <code>rowNumber</code> darab sort adja vissza.
     *
     * @param firstRow
     * @param rowNumber
     * @return
     */
    private String createQuery(int firstRow, int rowNumber) {
        String query = createQuery();
        query += " limit " + firstRow + ", " + rowNumber;
        return query;
    }

    /**
     * Elkészíti a partíciókat a lekérdezés eredményének minden attribútumára.
     *
     * @throws java.sql.SQLException
     */
    private void createPartitions() throws SQLException {
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(jdbcUrl);
            numberOfRows = retreiveTableSize(conn);
            st = conn.createStatement();

            String query = chunks ? createQuery(0, chunkSize) : createQuery();

            ResultSet results = st.executeQuery(query);
            Partitioner partitioner = new Partitioner(results);
            partitioner.partition();

            partitions = partitioner.getPartitions();
            if (chunks) {
                int size = sampled ? sampleSize : numberOfRows;
                for (int i = chunkSize; i <= size; i += chunkSize) {
                    results = st.executeQuery(createQuery(i, chunkSize));
                    partitioner = new Partitioner(results);
                    partitioner.partition(i);
                    Map<String, Partition> partitionsofit = partitioner.getPartitions();
                    for (String key : partitionsofit.keySet()) {
                        Partition newPartition = partitions.get(key).union(partitionsofit.get(key));
                        partitions.put(key, newPartition);
                    }
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (st != null) {
                st.close();
            }
        }
    }

    /**
     * Vissaadja a tábla sorainak számát.
     *
     * @param conn Az adatbáziseléréshez használt kapcsolat.
     * @return A tábla sorainak száma.
     */
    private int retreiveTableSize(Connection conn) {
        Statement st = null;
        int rowNumber = -1;
        try {
            st = conn.createStatement();
            ResultSet cnt = st.executeQuery("select count(*) from " + table);
            while (cnt.next()) {
                rowNumber = cnt.getInt(1);
            }
        } catch (SQLException ex) {
            Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException ex) {
                    Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return rowNumber;
    }

    /**
     * Megkapja attribútumoknak egy listáját és azokból generálja a következő szintet.
     * A lista nem csak attribútumokat tartalmazhat, hanem attribútum halmazokat is.
     * Az atribútum halmazokat reprezentáló sztringekben az attribútumokat egy : választja el.
     * A metódus feltételezi, hogy az attribútumok rendezettek.
     *
     * @param level Egy adott szint attribútumait/attribútum halmazait tartalmazó lista.
     * @return A következő szint atribútumait/attribútum halmazait tartalmazó lista.
     */
    private List<String> generateNextLevel(List<String> level, int levelNumber) {
        if (level == null || level.size() == 0) {
            return null;
        }

        List<String> result = new ArrayList<String>();

        //külön kezeljük az első szintet..
        if (levelNumber == 1) {
            int size = level.size();
            for (int i = 0; i < size; i++) {
                String levelI = level.get(i);
                for (int j = i + 1; j < size; j++) {
                    result.add(levelI + ":" + level.get(j));
                }
            }

            //logger.info("level generation from " + level + ": " + result);
            return result;
        }

        //..és a többit
        Map<String, List<String>> blocks = prefixBlocks(level, levelNumber);

        for (Map.Entry<String, List<String>> entry : blocks.entrySet()) {
            List<String> suffixes = entry.getValue();
            String key = entry.getKey() + ":";
            int size = suffixes.size();
            for (int i = 0; i < size; i++) {
                String suffixI = key + suffixes.get(i) + ":";
                for (int j = i + 1; j < size; j++) {//TODO: ez itt nagyon szar, át kéne gondolni az egészet
                    String candidate = suffixI + suffixes.get(j);
                    String[] parts = candidate.split(":");
                    boolean containsAll = true;
                    for (String part : parts) {
                        String newCandidate = null;
                        if (candidate.endsWith(part)) {
                            newCandidate = candidate.replace(":" + part, "");
                        } else {
                            newCandidate = candidate.replace(part + ":", "");
                        }
                        if (!level.contains(newCandidate)) {
                            containsAll = false;
                            break;
                        }
                    }
                    if (containsAll) {
                        result.add(candidate);
                    }
                }
            }
        }

        //TODO: ezt időoptimalizáció miatt vettem ki, memória miatt még szükség lehet rá.
        //blocks.clear();
        //System.gc();
        //logger.info("level generation from " + level + ": " + result);
        return result;
    }

    /**
     * Attribútumok egy listájához kiszámítja a prefix blokkokat. Az eredmény egy map,
     * amiben a kulcsok a prefixek, és minden kulcshoz tartozik egy lista, amiben az
     * ahhoz kapcsolódó suffixek vannak.
     * 
     * @param level
     * @return
     */
    private Map<String, List<String>> prefixBlocks(List<String> level, int levelNumber) {
        Map<String, List<String>> result = new TreeMap<String, List<String>>();

        //külön kezeljük az első szintet..
        if (levelNumber == 1) {
            for (String attribute : level) {
                List<String> t = new ArrayList<String>();
                t.add(attribute);
                result.put(attribute, t);
            }

            return result;
        }

//..és a többit
        for (String attributeList : level) {
            int index = attributeList.lastIndexOf(":");
            String prefix = attributeList.substring(0, index);
            String suffix = attributeList.substring(index + 1);
            //System.out.println(prefix + ", " + suffix);
            List<String> block = result.get(prefix);
            if (block != null) {
                block.add(suffix);
            } else {
                List<String> t = new ArrayList<String>();
                t.add(suffix);
                result.put(prefix, t);
            }
        }

        return result;
    }

    /**
     * Maga a fő algoritmus.
     */
    public void proceed() throws SQLException {
        int l = 1; //hányadik szinten járunk

        createPartitions();
        sortBasePartitions();

        List<String> level = new ArrayList<String>(attributes); //ebből még fogunk eltávolítani
        for (Iterator<String> it = level.iterator(); it.hasNext();) {
            String att = it.next();
            if (partitions.get(att).getNumberOfClasses() == numberOfRows) {
                it.remove();
            }
        }

        List<String> candidates = new ArrayList<String>();
        candidates.addAll(level);
        candidateLists = new HashMap<String, List<String>>();
        candidateLists.put("", candidates);

        while (level != null && level.size() != 0) {
            computeDependencies(level, l);
            cleanPartitions(l);
            level = prune(level);
            level = generateNextLevel(level, l);
            l++;
        }
    }

    /**
     * Kiszámítja a függőségeket, azaz feltölti a <code>dependencies</code> mapet.
     *
     * @param level Az aktuális szint.
     */
    private void computeDependencies(List<String> level, int levelNumber) {
        Map<String, List<String>> newCandidates = new HashMap<String, List<String>>();

        //a jelölthalmazok kiszámítása
        for (String attributeList : level) {
            List<String> candidateList = null;
            List<String> subs = subSets(attributeList, levelNumber);
            int i = 0;
            for (; i < subs.size(); i++) {
                if (candidateLists.get(subs.get(i)) != null) {
                    candidateList = new ArrayList<String>(candidateLists.get(subs.get(i)));
                    i++;
                    break;
                }
            }
            for (; i < subs.size(); i++) {
                if (candidateLists.get(subs.get(i)) != null) {
                    candidateList.retainAll(candidateLists.get(subs.get(i)));
                }
            }

            //TODO:itt eredetileg nem ez állt és lehet nem is kéne
            newCandidates.put(attributeList, candidateList);
        }

        candidateLists = newCandidates;

        //a függőségek tényleges kiszámítása
        for (String attributeList : level) { //for X eleme L
            List<String> subs = Arrays.asList(attributeList.split(":"));
            List<String> candidateList = candidateLists.get(attributeList);
            if (candidateList != null && candidateList.size() > 0) {
                for (String att : subs) {
                    possibleDependencies++;

                    if (candidateList.contains(att)) {
                        String dep = attributeListMinusAttribute(attributeList, att) + "->" + att;
                        Collection<Integer> toDelete = checkDependency(dep); //TODO: ezen a sztringkezelésden javítani, nameg ezt implementálni

                        if (toDelete != null) {
                            int deletand = toDelete.size();
                            if ((double) (deletand) / numberOfRows <= getEpsilon()) { //ha jó
                                if (dependencies == null) {
                                    dependencies = new ArrayList<String>();
                                }
                                dependencies.add(dep);
                                candidateList.remove(att);
                                deletandMap.put(dep, toDelete);

                                if (deletand == 0) {
                                    for (String attributeInR : attributes) {
                                        if (!attributeList.contains(attributeInR)) {
                                            candidateList.remove(attributeInR);
                                            logger.info("removing attribute from candidate list: " + attributeInR);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    /**
     * Leellenőrzi, hogy egy függőség teljesül-e. Ez azt jelenti, hogy visszaad egy
     * listát, amiben benne vannak azok a sorok, amiket törölni kell a függőség
     * érvényessé válásához.
     *
     * @param dep A függőség.
     * @return A törlendő sorok sorszámát tartalmazó lista.
     */
    private Collection<Integer> checkDependency(String dep) {
        logger.info("checking dependency " + dep);
        String[] parts = dep.split("->");
        if (parts[0].isEmpty()) {
            return null;
        }

        ++dependenciesChecked;

        Partition leftPartition = partitions.get(parts[0]);

        if (leftPartition == null) {
            int index = parts[0].lastIndexOf(":");
            String first = parts[0].substring(0, index);
            String second = parts[0].substring(index + 1);
            leftPartition = partitions.get(first).multiply(partitions.get(second));
            partitions.put(parts[0], leftPartition);
        }
        Partition rightPartition = partitions.get(parts[0] + ":" + parts[1]);
        if (rightPartition == null) {
            rightPartition = leftPartition.multiply(partitions.get(parts[1]));
            partitions.put(parts[0] + ":" + parts[1], rightPartition);
        }

        Collection<Integer> toDel = leftPartition.getRowsToDelete(rightPartition);

        return toDel;
    }

    /**
     * Előállítja egy attribútumlista összes eggyel kevesebb attribútumot tartalmazó
     * részhalmazát. A listában az attribútumok :-tal vannak elválasztva.
     *
     * @param attrbuteList Az attribútumlista.
     * @return A részhalmazok listája.
     */
    private List<String> subSets(String attrbuteList, int levelNumber) {
        if (levelNumber == 1) {
            return Collections.singletonList("");
        }
        List<String> result = new ArrayList<String>();
        String[] parts = attrbuteList.split(":");
        for (String part : parts) {
            if (attrbuteList.endsWith(part)) {
                result.add(attrbuteList.replace(":" + part, ""));
            } else {
                result.add(attrbuteList.replace(part + ":", ""));
            }
        }

        return result;
    }

    /**
     * Egy atribútumlistából vivesz egy attribútumot és visszaadja az így kapott
     * attribútumlistát.
     *
     * @param attributeList A csonkítandó lista.
     * @param attribute Az attribútum, amit el kell távolítani.
     * @return <code>attributeList\attribute</code>.
     */
    private String attributeListMinusAttribute(String attributeList, String attribute) {
        if (attributeList.equals(attribute)) {
            return "";
        }
        if (attributeList.endsWith(attribute)) {
            return attributeList.replace(":" + attribute, "");
        }

        return attributeList.replace(attribute + ":", "");
    }

    /**
     * Elvégzi a szint metszését és visszadja az új szintet.
     *
     * @param level
     * @return
     */
    private List<String> prune(List<String> level) {
        //TODO: itt is az optimlizáció miatt lett ez linkedlist
        List<String> result = new LinkedList<String>(level);
        for (int i = 0; i < result.size(); i++) {
            String attributeList = level.get(i);
            List<String> candidateList = candidateLists.get(attributeList);
            if (candidateList == null || candidateList.size() == 0) {
                result.remove(attributeList);
            }

            Partition partition = partitions.get(attributeList); //TODO: ezt a feltételt a dogába is beletenni
            if (partition != null && (partition.getClasses().size() + partition.getStrippedRows()) == numberOfRows) {
                result.remove(attributeList);
            }
        }

        return result;
    }

    /**
     * Ha mintát használtunk, akkor ezzel a metódussal verifikálhatjuk a 
     * megtalált függőségeket.
     */
    public void verifyDependencies() {
        boolean oldChunks = chunks;
        chunks = false;
        Connection conn = null;
        Statement st = null;
        try {
            conn = DriverManager.getConnection(jdbcUrl); //TODO: kivenni a literálokat

            st = conn.createStatement();
            int rowNumber = retreiveTableSize(conn);
            st = conn.createStatement();
            ResultSet results = st.executeQuery(createQuery());
            ResultSetMetaData meta = (ResultSetMetaData) results.getMetaData();
            int columnCount = meta.getColumnCount();
            Object[][] data = new Object[rowNumber][columnCount];
            int row = 0;
            while (results.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    data[row][i - 1] = results.getObject(i);
                }
                ++row;
            }


            for (String dependency : dependencies) {
                Collection<Integer> toDelete = deletandMap.get(dependency);
                String newDep = dependency.replace("->", ":");
                Partition p = partitions.get(newDep);
                List<Integer> rows = new ArrayList<Integer>();
                for (EquivalenceClass<Object, Integer> clazz : p.getClasses()) {
                    rows.add(clazz.getRows().get(0));
                }
                String[] parts = newDep.split(":");
                int[] attributeIndexes = new int[parts.length];

                for (int i = 0; i < parts.length; i++) { //el kell rakni, hogy az egyeds attribútumok mely oszlophoz tartoznak
                    attributeIndexes[i] = attributes.indexOf(parts[i]);
                }
                for (int i = 0; i < data.length; i++) {
                    if (toDelete.contains(i)) //ezeket már lehet a lekérdezésnél ki kellene hagyni
                    {
                        continue;
                    }

                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
        }

        chunks = oldChunks;
    }
//</editor-fold>

    public void addAttribute(String attribute) {
        if (attributes == null) {
            attributes = new ArrayList<String>();
        }

        attributes.add(attribute);
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    /**
     * @return the dependencies
     */
    public List<String> getDependencies() {
        return dependencies;
    }

    /**
     * @return the epsilon
     */
    public double getEpsilon() {
        return epsilon;
    }

    /**
     * @param epsilon the epsilon to set
     */
    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    /**
     * @return the delta
     */
    public double getDelta() {
        return delta;
    }

    /**
     * @param delta the delta to set
     */
    public void setDelta(double delta) {
        this.delta = delta;
    }

    /**
     * @return the sampled
     */
    public boolean isSampled() {
        return sampled;
    }

    /**
     * @param sampled the sampled to set
     */
    public void setSampled(boolean sampled) {
        this.sampled = sampled;
    }

    /**
     * @return the chunks
     */
    public boolean isChunks() {
        return chunks;
    }

    /**
     * @param chunks the chunks to set
     */
    public void setChunks(boolean chunks) {
        this.chunks = chunks;
    }

    /**
     * @return the chunkSize
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * @param chunkSize the chunkSize to set
     */
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    /**
     * Előállít egy help stringet az alkalmazás parancssori opcióiról.
     * 
     * @return
     */
    private String getUsage() {
        StringBuilder builder = new StringBuilder("Haszálat: java ex.tajti.mining.Cleaner -t <tábla> -a <attribútumok> [opciók]\n");
        builder.append("<tábla>: a tisztítandó tábla neve\n");
        builder.append("<attribútumok>: azoknak az attribútumoknak a vesszővel elválasztott neve, amiknek szerepelniük kell a lekédrdezésben\n");
        builder.append("Opciók:\n");
        builder.append("-help: megjeleníti ezt a szöveget\n");
        builder.append("-j url: a JDBC url, amin keresztül az adatbázis elérhető. Megadása kötelező\n");
        builder.append("-s: Ha nincs megadva, akkor a teljes adatbázis tartalmát feldolgozza, különben mintát használ.\n");
        builder.append("-jd driver: a JDBC driver osztály teljesen minősített neve.  Kötelező megadni. Jelenleg csak MySQL-lel működik.\n");
        builder.append("-d delta: delta a mintavételezésnél használt érték. Az alapételmezett érték 0.05.\n");
        builder.append("-c n: n méretű részrelációnként dolgozza fel a tábla adatait\n");
        builder.append("-e epszilon: az epszilon értéke. Megadása kötelező. Az alapértelmezett érték 0.05.\n");

        return builder.toString();
    }

    /**
     * Feldolgozza a parancssori argumentumokat és visszaad egy megfelelően beállított
     * <code>Cleaner</code> objektumot.
     *
     * @param args A parancssori argumentumok.
     */
    private void processCommandLine(String[] args) {
        //Cleaner cleaner = new Cleaner();
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-help")) {
                String usage = getUsage();
                System.out.println(usage);
            } else if (args[i].equals("-t")) {
                this.table = args[i + 1];
                ++i;
            } else if (args[i].equals("-a")) {
                String atts = args[i + 1];
                String[] parts = atts.split(",");
                attributes = new ArrayList<String>();
                for (int j = 0; j < parts.length; j++) {
                    attributes.add(parts[j]);
                }
                ++i;
            } else if (args[i].equals("-e")) {
                String eps = args[i + 1];
                epsilon = Double.parseDouble(eps);

                ++i;
            } else if (args[i].equals("-d")) {
                String del = args[i + 1];
                delta = Double.valueOf(del);

                ++i;
            } else if (args[i].equals("-s")) {
                String size = args[i + 1];
                sampled = true;
            } else if (args[i].equals("-c")) {
                String chunksize = args[i + 1];
                chunkSize = Integer.valueOf(chunksize);
                chunks = true;

                i++;
            } else if (args[i].equals("-j")) {
                jdbcUrl = args[i + 1];

                ++i;
            } else if (args[i].equals("-jd")) {
                jdbcDriver = args[i + 1];
                try {
                    Class.forName(jdbcDriver);
                } catch (ClassNotFoundException ex) {
                    System.out.println("A driver osztály nem található.");
                    Logger.getLogger(Cleaner.class.getName()).log(Level.SEVERE, null, ex);
                }

                ++i;
            }
        }

        if (jdbcDriver == null || jdbcUrl == null || attributes == null || table == null) {
            System.out.println("A -jd, -j, -a és -t opciók használata kötelező.");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws SQLException {
        Cleaner tane = new Cleaner();
        tane.processCommandLine(args);

        System.out.println(tane.attributes);

        // tane.verifyDependencies();
        long beginning = System.currentTimeMillis();
        tane.proceed();

        System.out.println("Iterations: " + Partition.iterations);
        System.out.println("Stripped: " + Partition.stripped);
        System.out.println(Partition.continued);
        StringBuilder builder = new StringBuilder();
        builder.append("================ General ============\n").append("Date: " + new Date() + "\n");
        builder.append("============= Statistics ============\n").append("Time elapsed: " + (System.currentTimeMillis() - beginning) + "\n").append("Number of rows: " + tane.numberOfRows + "\n").append("Sample size: " + (tane.sampled ? tane.sampleSize : "not sampled") + "\n").append("Chunk size: " + (tane.chunks ? tane.chunkSize : "not chunked") + "\n").append("Table: " + tane.table + "\n").append("Attribute count: " + tane.attributes.size() + "\n").append("Epsilon: " + tane.epsilon + "\n").append("Delta: " + tane.delta + "\n").append("Possible dependencies: " + possibleDependencies + "\n").append("Dependencies checked: " + dependenciesChecked + "\n").append("Dependencies found: " + (tane.getDependencies() == null ? 0 : tane.getDependencies().size()) + "\n");
//        System.out.println("EREDMÉNY: " + tane.getDependencies());
//        System.out.println("a törlendő sorok:\n" + tane.deletandMap);
        if (tane.getDependencies() != null) {
            builder.append("========== Dependencies =============\n");
            for (String dep : tane.getDependencies()) {
                builder.append("Dependency: " + dep + "\n");
                builder.append("To delete (" + tane.deletandMap.get(dep).size() + "): " + tane.deletandMap.get(dep) + "\n");
            }

        }
        System.out.println(builder);
        serializeResults(builder);
    }
}
