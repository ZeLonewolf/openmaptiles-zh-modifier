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

import com.github.houbb.opencc4j.util.ZhConverterUtil;

public class OMTZHModifier {

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
	}

	private static void process(Connection c) throws SQLException {

		DatabaseMetaData metaData = c.getMetaData();
		String[] types = { "TABLE" };

		List<String> tables = new ArrayList<>();
		ResultSet rs = metaData.getTables(null, null, "%", types);
		while (rs.next()) {
			tables.add(rs.getString("TABLE_NAME"));
		}

		List<String> idTablesToModify = new ArrayList<>();
		List<String> osmTablesToModify = new ArrayList<>();

		for (String table : tables) {
			ResultSet resultSet = metaData.getColumns(null, null, table, null);
			boolean hasId = false;
			boolean hasOsmId = false;
			boolean hasName = false;
			boolean hasTags = false;
			while (resultSet.next()) {
				String name = resultSet.getString("COLUMN_NAME");
				if (name.equals("osm_id")) {
					hasOsmId = true;
				}
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
				idTablesToModify.add(table);
			} else if (hasOsmId && hasName && hasTags) {
				osmTablesToModify.add(table);
			}
		}

		System.out.println("Found " + idTablesToModify.size() + " tables(id) to update");
		System.out.println("Found " + osmTablesToModify.size() + " tables(osm_id) to update");

		idTablesToModify.forEach(table -> indexAndProcessTable(table, "id", c));
		osmTablesToModify.forEach(table -> indexAndProcessTable(table, "osm_id", c));

	}

	private static void indexAndProcessTable(String table, String idField, Connection c) {

		try {
			Statement stmt = c.createStatement();

			String dropIndexSQL = "drop index if exists temp_zh_id;";
			String createIndexSQL = "create index temp_zh_id on " + table + "(" + idField + ");";

			System.out.print("Indexing " + table + "...");
			stmt.execute(dropIndexSQL);
			System.out.println(dropIndexSQL);
			stmt.execute(createIndexSQL);
			System.out.println(createIndexSQL);
			System.out.println("done!");
			processTable(c, table, idField);
			stmt.execute(dropIndexSQL);
			System.out.println(dropIndexSQL);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}

	private static long getMaxID(Connection c, String table, String idField) throws SQLException {
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select max(" + idField + ") as max_id from " + table + ";");
		rs.next();
		return rs.getLong("max_id");
	}

	private static NumberFormat fmtPct = new DecimalFormat("##.##");

	private static void processTable(Connection c, String table, String idField) throws SQLException {

		System.out.println("Adding zh tags to [" + table + "]");

		long maxID = getMaxID(c, table, idField);

		long size = 20000;

		if (idField.equals("osm_id")) {
			size = maxID / 100;
		}

		Statement s = c.createStatement();

		for (long i = 0; i < maxID; i += size) {
			System.out.println("Batch " + i + " to " + (i + size - 1) + " of " + String.valueOf(maxID) + " ("
					+ fmtPct.format(100f * i / maxID) + "%) -- " + table);

			ResultSet rs = s
					.executeQuery("select " + idField + ", name, tags->'name:zh' as zh, tags->'name:zh-Hans' as hans, "
							+ "tags->'names:zh-Hant' as hant from " + table + " where " + idField + " BETWEEN " + i
							+ " AND " + (i + size - 1) + " AND (name is NOT NULL or tags->'name:zh' IS NOT NULL) AND "
							+ "(tags->'name:zh-Hant' IS NULL OR tags->'name:zh-Hans' IS NULL);");

			List<ChineseValues> updates = new ArrayList<>();
			while (rs.next()) {
				ChineseValues results = processRecord(rs, idField);
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
							+ "'::hstore || 'name:zh-Hant => " + hstoreEscape(cv.getHant()) + "'::hstore WHERE "
							+ idField + " = " + cv.getId())
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
		try {
			return org.postgresql.core.Utils.escapeLiteral(null, s, false).toString().replaceAll("\\s+", "\\\\ ")
					.replace(",", "\\,").replace("\"", "\\\"");
		} catch (SQLException e) {
			e.printStackTrace();
			// Something terrible has happened.
			System.exit(0);
			return s;
		}
	}

	private static ChineseValues processRecord(ResultSet rs, String idField) throws SQLException {
		return processRecord(rs.getLong(idField), rs.getString("name"), rs.getString("zh"), rs.getString("hans"),
				rs.getString("hant"));
	}

	private static ChineseValues processRecord(long id, String name, String izh, String ihans, String ihant) {

		String zh = izh;
		String hans = ihans;
		String hant = ihant;

		boolean needsUpdate = false;

		if (zh == null) {
			if (name == null || name.isEmpty()) {
				return null;
			}
			if (isHanScript(name)) {
				zh = name;
			} else {
				return null;
			}
		}

		if (hans != null && hans.isEmpty()) {
			hans = null;
		}
		if (hant != null && hant.isEmpty()) {
			hant = null;
		}

		if (hans == null) {
			hans = ZhConverterUtil.toSimple(zh);
			needsUpdate = true;
		}

		if (hant == null) {
			hant = ZhConverterUtil.toTraditional(zh);
			needsUpdate = true;
		}

		if (needsUpdate) {
			ChineseValues cv = new ChineseValues();
			cv.setHans(hans);
			cv.setHant(hant);
			cv.setId(id);
			return cv;
		}
		return null;
	}

	public static boolean isHanScript(String s) {
		return s.codePoints()
				.anyMatch(codepoint -> Character.UnicodeScript.of(codepoint) == Character.UnicodeScript.HAN);
	}
}
