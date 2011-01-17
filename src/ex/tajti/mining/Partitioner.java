package ex.tajti.mining;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Egy lekérdezés eredménye alapján elkészíti a kiindulási partíciókat, azaz
 * azokat, amelyek az egyelemű attribútum halmazokhoz tartoznak.
 *
 * @author tajti ákos
 */
public class Partitioner {

    /**
     * A <code>ResultSet</code> objektum, amivel a lekérdezés eredményét elérjük.
     */
    private ResultSet results;
    /**
     * Az attribútumokat és a hozzájuk tartozó partíciókat összerendelő map.
     */
    private Map<String, Partition> partitions;
    /**
     * Az attribútumok nevei.
     */
    private String[] columnNames;
    /**
     * A lekérdezés metainformációi.
     */
    private ResultSetMetaData meta;
    private Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    /**
     * A ledkérdezés eredényében visszakapott sorok száma.
     */
    private Integer numberOfRows;
//    private int sampleSize;

    /**
     * Példányosítja az osztályt.
     *
     * @param results
     * @throws java.sql.SQLException
     */
    public Partitioner(ResultSet results) throws SQLException {
        this.results = results;
        meta = results.getMetaData();
        columnNames = new String[meta.getColumnCount() + 1];
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            columnNames[i] = meta.getColumnName(i);
        }
        partitions = new HashMap<String, Partition>();
    }

    /**
     * Elkészíti a partíciókat és az automatikusan generált sorazonosítók közül
     * az első értéke <code>i</code>.
     *
     * @param i
     * @throws java.sql.SQLException
     */
    public void partition(int i) throws SQLException {
        preparePartitions();
        processResult(i);
    }

    /**
     * Elkészíti a partíciókat.
     *
     * @throws java.sql.SQLException
     */
    public void partition() throws SQLException {
        partition(0);
    }

    /**
     * Feldolgozza a resultsetet. Minden sorhoz generál egy azonosítót. <code>j</code>
     * az első sorazonosító értéke.
     *
     * @param j
     * @return
     * @throws java.sql.SQLException
     */
    private int processResult(int j) throws SQLException {
        logger.fine("processResult");
        while (results.next()) {
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                getPartitions().get(columnNames[i]).addRow(j, results.getObject(i));
            }
            j++;
        }

        results.close();
        numberOfRows = j;
        return j;
    }

    /**
     * Feldolgozza a resultsetet. Minden sorhoz automatikusan generál egy azonosítót.
     * Az első azonosító értéke 0.
     *
     * @return
     * @throws java.sql.SQLException
     */
    private int processResult() throws SQLException {
        return processResult(0);
    }

    /**
     * Elvégzi a partíciók elészítéséhez szükséges műveleteket.
     *
     * @throws java.sql.SQLException
     */
    private void preparePartitions() throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            partitions.put(meta.getColumnName(i), new Partition(meta.getColumnName(i)));
        }
    }

    /**
     * Visszaadja a kiszámított partíciókat. A mapben a kulcs egy attribútum neve,
     * az érték pedig az attribútumhoz tartozó partíció.
     *
     * @return 
     */
    public Map<String, Partition> getPartitions() {
        return partitions;
    }

    /**
     * Visszaadja a lekérdezés sorainak számát.
     *
     * @return
     */
    public Integer getNumberOfRows() {
        return numberOfRows;
    }
}
