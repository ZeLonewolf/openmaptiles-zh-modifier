package com.streetferret;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ibm.icu.text.Transliterator;

public class OMTZHModifier {

	private static Transliterator st = Transliterator.getInstance("Hans-Hant");
	private static Transliterator ts = Transliterator.getInstance("Hant-Hans");
	private static Transliterator at = Transliterator.getInstance("Any-Hant");
	private static Transliterator as = Transliterator.getInstance("Any-Hans");

	public static void main(String[] args) {

		Connection c = null;
		try {
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection("jdbc:postgresql://localhost:5432/openmaptiles", "openmaptiles",
					"openmaptiles");
			process(c);
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

		// Required indexes:
		// create index oal_id on osm_aerialway_linestring(id)
		// create index ohl_id on osm_highway_linestring(id)
		// create index opp_id on osm_poi_point(id)
		// create index oppg_id on osm_poi_polygon(id)
		// create index owl_id on osm_railway_linestring(id)
	}

	private static void process(Connection c) throws SQLException {

		DatabaseMetaData metaData = c.getMetaData();
		String[] types = { "TABLE" };

		List<String> tables = new ArrayList<>();
		ResultSet rs = metaData.getTables(null, null, "%", types);
		while (rs.next()) {
			tables.add(rs.getString("TABLE_NAME"));
		}

		List<String> tablesToModify = new ArrayList<>();
		for (String table : tables) {
			ResultSet resultSet = metaData.getColumns(null, null, table, null);
			boolean hasId = false;
			boolean hasName = false;
			boolean hasTags = false;
			while (resultSet.next()) {
				String name = resultSet.getString("COLUMN_NAME");
				if (name.equals("id")) {
					hasId = true;
				}
				if (name.equals("name")) {
					hasName = true;
				}
				if (name.equals("tags")) {
					hasTags = true;
				}
			}
			if (hasId && hasName && hasTags) {
				tablesToModify.add(table);
			}
		}

		System.out.println("Found " + tablesToModify.size() + " tables to update");

		Statement stmt = c.createStatement();
		
		tablesToModify.forEach(table -> {

			String dropIndexSQL = "drop index if exists temp_zh_id;";
			String createIndexSQL = "create index temp_zh_id on " + table + "(id);";
			System.out.println(createIndexSQL);
			
			try {
				System.out.print("Indexing " + table + "...");
				stmt.execute(dropIndexSQL);
				stmt.execute(createIndexSQL);
				System.out.println("done!");
				processTable(c, table);
				stmt.execute(dropIndexSQL);
			} catch (SQLException e) {
				e.printStackTrace();
				System.exit(0);
			}
		});

	}

	private static long getMaxID(Connection c, String table) throws SQLException {
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select max(id) as max_id from " + table + ";");
		rs.next();
		return rs.getLong("max_id");
	}

	private static NumberFormat fmtPct = new DecimalFormat("##.##");

	private static void processTable(Connection c, String table) throws SQLException {

		System.out.println("Adding zh tags to [" + table + "]");

		long maxID = getMaxID(c, table);

		long size = 20000;

		Statement s = c.createStatement();

		for (long i = 0; i < maxID; i += size) {
			System.out.println("Batch " + i + " to " + (i + size - 1) + " of " + String.valueOf(maxID) + " ("
					+ fmtPct.format(100f * i / maxID) + "%) -- " + table);

			ResultSet rs = s
					.executeQuery("select id, osm_id, name, tags->'name:zh' as zh, tags->'name:zh-Hans' as hans, "
							+ "tags->'names:zh-Hant' as hant from " + table + " where id BETWEEN " + i + " AND "
							+ (i + size - 1) + " AND (name is NOT NULL or tags->'name:zh' IS NOT NULL) AND "
							+ "(tags->'name:zh-Hant' IS NULL OR tags->'name:zh-Hans' IS NULL);");

			List<ChineseValues> updates = new ArrayList<>();
			while (rs.next()) {
				ChineseValues results = processRecord(rs);
				if (results != null) {
					updates.add(results);
				}
			}

			rs.close();

			if (!updates.isEmpty()) {
				System.out.println("  Found " + updates.size() + " matches");
			}

			long timing = System.currentTimeMillis();

			updates.stream()
					.map(cv -> "update " + table + " SET tags = tags || 'name:zh-Hans => " + hstoreEscape(cv.getHans())
							+ "'::hstore || 'name:zh-Hant => " + hstoreEscape(cv.getHant()) + "'::hstore WHERE id = "
							+ cv.getId())
					.forEach(sql -> {
						try {
							s.addBatch(sql);
						} catch (SQLException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					});

			int[] results = s.executeBatch();
			int updateCount = Arrays.stream(results).sum();

			if (updateCount > 0) {
				timing = System.currentTimeMillis() - timing;
				int recPerSec = (int) ((float) updateCount / (timing / 1000f));
				System.out.println("  Updated " + updateCount + " records, " + recPerSec + "/s");
			}

			s.clearBatch();
		}

		s.close();
	}

	private static String hstoreEscape(String s) {
		return s.replace(" ", "\\ ").replace("'", "''").replace(",", "\\,");
	}

	private static ChineseValues processRecord(ResultSet rs) throws SQLException {

		String name = rs.getString("name");
		String zh = rs.getString("zh");
		boolean needsUpdate = false;

		if (zh == null) {
			if (name == null || name.isEmpty()) {
				return null;
			}
			if (allHanScript(name)) {
				zh = name;
			} else {
				return null;
			}
		}

		String hans = rs.getString("hans");
		String hant = rs.getString("hant");

		if (hans != null && hans.isEmpty()) {
			hans = null;
		}
		if (hant != null && hant.isEmpty()) {
			hant = null;
		}

		if (hans == null) {
			String hansTrans = as.transliterate(zh);
			if (zh.equals(hansTrans)) {
				hans = hansTrans;
				needsUpdate = true;
			}
		}

		if (hant == null) {
			String hantTrans = at.transliterate(zh);
			if (zh.equals(hantTrans)) {
				hant = hantTrans;
				needsUpdate = true;
			}
		}

		if (hans == null && hant != null) {
			hans = ts.transliterate(hant);
		}

		if (hant == null && hans != null) {
			hant = st.transliterate(hans);
		}

		if (hans != null && hant != null && !hans.equals(hant)) {
		}

		if (needsUpdate) {
//			if (!hans.equals(hant)) {
//				System.out.println("Testing: [" + zh + "]");
//				System.out.println(" Hans: [" + hans + "]");
//				System.out.println(" Hant: [" + hant + "]");
//				System.out.println(" OSM: [" + rs.getLong("osm_id") + "]");
//				System.out.println(" ID: [" + rs.getLong("id") + "]");
//			}

			ChineseValues cv = new ChineseValues();
			cv.setHans(hans);
			cv.setHant(hant);
			cv.setId(rs.getLong("id"));

			return cv;
		}
		return null;
	}

	public static boolean allHanScript(String s) {
		return s.codePoints()
				.anyMatch(codepoint -> Character.UnicodeScript.of(codepoint) == Character.UnicodeScript.HAN);
	}
}
