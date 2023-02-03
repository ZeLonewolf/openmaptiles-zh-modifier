package com.streetferret;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.ibm.icu.text.Transliterator;

public class OMTZHModifier {

	private static Transliterator st = Transliterator.getInstance("Hans-Hant");
	private static Transliterator ts = Transliterator.getInstance("Hant-Hans");
	private static Transliterator at = Transliterator.getInstance("Any-Hant");
	private static Transliterator as = Transliterator.getInstance("Any-Hans");

	public static void main(String[] args) {

//		Enumeration<String> ids = com.ibm.icu.text.Transliterator.getAvailableIDs();
//
//		while(ids.hasMoreElements()) {
//			System.out.println(ids.nextElement());
//		}
//		System.exit(0);

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
	}

	private static void process(Connection c) throws SQLException {
		processTable(c, "osm_highway_linestring");
	}

	private static long getMaxID(Connection c, String table) throws SQLException {
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select max(id) as max_id from " + table + ";");
		rs.next();
		return rs.getLong("max_id");
	}

	private static void processTable(Connection c, String table) throws SQLException {

		long maxID = getMaxID(c, table);

		long size = 100000;

		for (long i = 0; i < maxID; i += size) {
			System.out.println("Batch " + i + " to " + (i + size - 1));

			Statement s = c.createStatement();
			ResultSet rs = s
					.executeQuery("select id, osm_id, name, tags->'name:zh' as zh, tags->'name:zh-Hans' as hans, "
							+ "tags->'names:zh-Hant' as hant from " + table + " where id BETWEEN " + i + " AND "
							+ (i + size - 1) + " AND (tags->'name:zh-Hant' IS NULL OR tags->'name:zh-Hans' IS NULL);");

			List<ChineseValues> updates = new ArrayList<>();
			while (rs.next()) {
				ChineseValues results = processRecord(rs);
				if (results != null) {
					updates.add(results);
				}
			}

			s.close();
			
			System.out.println("Found " + updates.size() + " matches");

			updates.forEach(cv -> updateChineseValues(c, cv));
		}
	}

	private static void updateChineseValues(Connection c, ChineseValues cv) {
		String updateSQL = "update osm_highway_linestring SET tags = tags || 'name:zh-Hans => " + cv.getHans()
				+ "'::hstore || 'name:zh-Hant => " + cv.getHant() + "'::hstore WHERE id = " + cv.getId();
		try {
			Statement s = c.createStatement();
			s.execute(updateSQL);
			System.out.println(" -> update " + cv.getId());
		} catch (SQLException e) {
			e.printStackTrace();
		}
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
				.allMatch(codepoint -> Character.UnicodeScript.of(codepoint) == Character.UnicodeScript.HAN);
	}
}
