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
 * Implements the modified TANE algorithm. This version can be used to find rows breaking some
 * functional dependencies. These dependencies are computed based on the current contents of the
 * database.
 * <br/>
 * The description of the original TANE algorithm can be downloaded <a href="http://www.cs.helsinki.fi/research/fdk/datamining/tane/shortpaper.ps">here</a>.
 *
 * @author Akos Tajti
 */
public class Cleaner {

	static SimpleDateFormat format = new SimpleDateFormat("yMd-Hm");
	/**
	 * The name of the table the functional dependencies are searched on.
	 */
	private String table;

	/**
	 * The JDBC url to use.
	 */
	private String jdbcUrl;

	/**
	 * The list of the names of the attributes the algorithm has to consider. Mustn't
	 * contains key attributes.
	 */
	private List<String> attributes;

	/**
	 * The list of valid functional dependencies. The format of the dependencies:
	 * attr1:attr2->attr3
	 */
	private List<String> dependencies;

	/**
	 * Contains the candidate lists. The keys are attribute sets (represented as <code>String</code>),
	 * the entries are lists of candidates.
	 */
	private Map<String, List<String>> candidateLists;

	/**
	 * Contains the partitions. Unneeded partitions are deleted.
	 */
	private Map<String, Partition> partitions;

	/**
	 * If the number of the rows breaking a dependency divided by the total numkber of rows
	 * is less then this threshold than the dependency is valid.
	 */
	private double epsilon = 0.05;

	/**
	 * We use <code>delta</code> to compute the number of rows in the sample (when working with
	 * samples instead of all rows). This number represents the error we can live with when using
	 * samples. The lower the <code>delta</code> is the higher number of rows the sample must have.
	 */
	private double delta = 0.05;

	/**
	 * If <code>true</code> he algorithm uses just a portion of the rows.
	 */
	private boolean sampled;

	/**
	 * If <code>true</code> the algorithm processes the rows in chunks. This helps to
	 * prevent <code>OutOfMemoryError</code>s.
	 */
	private boolean chunks;

	/**
	 * Used when <code>chunks</code> is true.
	 */
	private int chunkSize;

	/**
	 * The number of rows in the result of the query.
	 */
	private int numberOfRows;

	private Logger logger = Logger.getLogger(this.getClass().getSimpleName());

	/**
	 * Contains the rows breaking dependencies. the keys are the dependencies (represented as <code>String</code>s).
	 * The values are collections of row ids.
	 */
	private Map<String, Collection<Integer>> deletandMap = new HashMap<String, Collection<Integer>>();

	/**
	 * The number of dependencies checked.
	 */
	public static int dependenciesChecked = 0;

	/**
	 * The number of possible dependencies (<code>possibleDependencies >= checkedDependencies</code>).
	 */
	public static int possibleDependencies = 0;

	/**
	 * The samplesize computed based on <code>delta</code> (and other things).
	 */
	private int sampleSize;

	/**
	 * The name of the JDBC driver.
	 */
	private String jdbcDriver;

	/**
	 * Removes the partitions that are not needed at level <code>levelNumber</code>.
	 *
	 * @param levelNumber
	 */
	private void cleanPartitions(int levelNumber) {
		for (Iterator<Entry<String, Partition>> it = partitions.entrySet().iterator(); it.hasNext();) {
			Entry<String, Partition> entry = it.next();

			String key = entry.getKey();
			// TODO: possible npe
			int levelOfPartition = entry.getValue().getLevel();
			if (levelOfPartition != 0 && levelOfPartition + 1 < levelNumber - 1) {
				logger.info("cleaning up partition for " + key);
				it.remove();
			}
		}
	}

	/**
	 * Sorts the partitions corresponding to base attributes.
	 */
	private void sortBasePartitions() {
		Collections.sort(attributes, new Comparator<String>() {

			public int compare(String o1, String o2) {
				Partition part1 = partitions.get(o1);
				Partition part2 = partitions.get(o2);

				// descending order based on the number of equivalence classes
				return part1.getNumberOfClasses() - part2.getNumberOfClasses();
			}
		});
	}

	/**
	 * Writes the results to a file in the reports directory (relative to the current working
	 * directory.
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

	//
	// The most important part of the code
	///

	/**
	 * Builds a query bassed on the tablename and the names of the attributes. Incremental
	 * processing (that is, processing the rows in chunks) is only supported for mysql.
	 *
	 *
	 * @return The query.
	 */
	private String createQuery() {
		// computing sample size
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

		// TODO: more sophisticated sample handling
		if (!chunks && sampled && sampleSize < numberOfRows) {
			builder.append(" limit 1," + sampleSize);
		}
		return builder.toString();
	}

	/**
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
	 * Creates the partitions for all attributes in the result of the query.
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
	 * Returns the number of rows in the table.
	 *
	 * @param conn
	 * @return
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
	 * Generates the left-sides to check on the next level based on the attributes and attibute sets
	 * in <code>level</code>. Attribute sets are represented as strings in this format: attr1:attr2:attr3.
	 * the method assumes that the attributes are ordered.
	 *
	 * @param level Egy adott szint attribútumait/attribútum halmazait tartalmazó lista.
	 * @return A következő szint atribútumait/attribútum halmazait tartalmazó lista.
	 */
	private List<String> generateNextLevel(List<String> level, int levelNumber) {
		if (level == null || level.size() == 0) {
			return null;
		}

		List<String> result = new ArrayList<String>();

		// the first level is handled separately
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

		// other cases
		Map<String, List<String>> blocks = prefixBlocks(level, levelNumber);

		for (Map.Entry<String, List<String>> entry : blocks.entrySet()) {
			List<String> suffixes = entry.getValue();
			String key = entry.getKey() + ":";
			int size = suffixes.size();
			for (int i = 0; i < size; i++) {
				String suffixI = key + suffixes.get(i) + ":";
				for (int j = i + 1; j < size; j++) { // TODO: refactor, rethink
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

		// TODO: removed for optimization, check is needed
		//blocks.clear();
		//System.gc();
		//logger.info("level generation from " + level + ": " + result);
		return result;
	}

	/**
	 * Computes the prefix blocks for a given attribute list. In the result the keys
	 * are the prefixes and the values are the corresponding suffixes.
	 *
	 * @param level
	 * @return
	 */
	private Map<String, List<String>> prefixBlocks(List<String> level, int levelNumber) {
		Map<String, List<String>> result = new TreeMap<String, List<String>>();

		// first level
		if (levelNumber == 1) {
			for (String attribute : level) {
				List<String> t = new ArrayList<String>();
				t.add(attribute);
				result.put(attribute, t);
			}

			return result;
		}

		// other cases
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
	 * The main algorithm.
	 */
	public void proceed() throws SQLException {
		int l = 1; // the level

		createPartitions();
		sortBasePartitions();

		List<String> level = new ArrayList<String>(attributes);
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
	 * Computes the dependencies and puts them to the <code>dependencies</code> map.
	 *
	 * @param level
	 * @param levelNumber
	 */
	private void computeDependencies(List<String> level, int levelNumber) {
		Map<String, List<String>> newCandidates = new HashMap<String, List<String>>();

		// generating candidate sets
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

			newCandidates.put(attributeList, candidateList);
		}

		candidateLists = newCandidates;

		// dependencies are computed here
		for (String attributeList : level) { // for X in L
			List<String> subs = Arrays.asList(attributeList.split(":"));
			List<String> candidateList = candidateLists.get(attributeList);
			if (candidateList != null && candidateList.size() > 0) {
				for (String att : subs) {
					possibleDependencies++;

					if (candidateList.contains(att)) {
						String dep = attributeListMinusAttribute(attributeList, att) + "->" + att;
						Collection<Integer> toDelete = checkDependency(dep); // TODO: change string handling

						if (toDelete != null) {
							int deletand = toDelete.size();
							if ((double) (deletand) / numberOfRows <= getEpsilon()) { // valid dependency
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
	 * Return the rows that break the dependency <code>dep</code>.
	 *
	 * @param dep
	 * @return
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
	 * Returns a help describing the usage and command line arguments.
	 *
	 * @return
	 */
	private String getUsage() {
		StringBuilder builder = new StringBuilder("Usage: java ex.tajti.mining.Cleaner -t <table> -a <attributes> [options]\n");
		builder.append("<table>: the name of the table to be cleaned\n");
		builder.append("<attributes>: the name of the attributes the query must contain.\n");
		builder.append("Options:\n");
		builder.append("-help: prints this help message\n");
		builder.append("-j url: the JDBC url of the database used. MANDATORY\n");
		builder.append("-s: processing all rows in the table can be time and memory consuming. If this option is specified the application processes"
			+ " only a portion of the rows. The number of rows computes based on epsilon and delta.\n");
		builder.append("-jd driver: the fully qualified name of the JDBC driver. Must be in the classpath  MANDATORY. (Currently works only with mysql)\n");
		builder.append("-d delta: the value used for computing the sample (see documentation). The default value is 0.05.\n");
		builder.append("-c n: process the table n chunks of n rows\n");
		builder.append("-e epsilon: the epsilon value (see documentation). MANDATORY 0.05.\n");

		return builder.toString();
	}

	/**
	 * Processes the command line arguments and creates a <code>Cleaner</code> instance.
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
		builder.append("============= Statistics ============\n").append("Time elapsed: " + (System.currentTimeMillis() - beginning) + "\n")
			.append("Number of rows: " + tane.numberOfRows + "\n").append("Sample size: " + (tane.sampled ? tane.sampleSize : "not sampled") + "\n")
			.append("Chunk size: " + (tane.chunks ? tane.chunkSize : "not chunked") + "\n").append("Table: " + tane.table + "\n")
			.append("Attribute count: " + tane.attributes.size() + "\n").append("Epsilon: " + tane.epsilon + "\n").append("Delta: " + tane.delta + "\n")
			.append("Possible dependencies: " + possibleDependencies + "\n").append("Dependencies checked: " + dependenciesChecked + "\n")
			.append("Dependencies found: " + (tane.getDependencies() == null ? 0 : tane.getDependencies().size()) + "\n");
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
