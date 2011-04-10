package ex.tajti.mining;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Creates the base partitions from the result of a query. Base partitions are partitions
 * based on attribute sets with one element.
 *
 * @author tajti ákos
 */
public class Partitioner {

    /**
     * The <code>ResultSet</code> containing the result of the query.
     */
    private ResultSet results;
    /**
     * Maps attributes to partitions.
     */
    private Map<String, Partition> partitions;
    /**
     * Names of the columns in the result set.
     */
    private String[] columnNames;
    /**
     * Metainformation gained from the result set.
     */
    private ResultSetMetaData meta;
    private Logger logger = Logger.getLogger(this.getClass().getSimpleName());
    /**
     * The number of rows in the result.
     */
    private Integer numberOfRows;
//    private int sampleSize;

    /**
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
	 * Creates the partitions. For each row an integer row id is generated. The first
	 * id will be <code>i</code>.
     *
     * @param i
     * @throws java.sql.SQLException
     */
    public void partition(int i) throws SQLException {
        preparePartitions();
        processResult(i);
    }

    /**
     * Creates the partitions.
     *
     * @throws java.sql.SQLException
     */
    public void partition() throws SQLException {
        partition(0);
    }

    /**
	 * Processes the result set and generates a row id for each row. The value of the first id
	 * is <code>j</code>.
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
     * The same as <code>processResult(0)</code>.
     *
     * @return
     * @throws java.sql.SQLException
     */
    private int processResult() throws SQLException {
        return processResult(0);
    }

    /**
     * Initializes the <code>partitions</code> map.
     *
     * @throws java.sql.SQLException
     */
    private void preparePartitions() throws SQLException {
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            partitions.put(meta.getColumnName(i), new Partition(meta.getColumnName(i)));
        }
    }

    /**
     * Returns the partitions. The key in each entry is an attribute name and 
	 * the value is a <code>Partition</code> object.
     *
     * @return 
     */
    public Map<String, Partition> getPartitions() {
        return partitions;
    }

    /**
     * Returns the number of rows in the result.
     *
     * @return
     */
    public Integer getNumberOfRows() {
        return numberOfRows;
    }
}
