package com.streetferret;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select id, tags->'name:zh' as zh, tags->'name:zh-Hans' as hans, "
				+ "tags->'names:zh-Hant' as hant from osm_highway_linestring where "
				+ "tags->'name:zh' IS NOT NULL AND tags->'name:zh-Hans' IS NULL AND tags->'name:zh:Hant' IS NULL LIMIT 100;");

		// TODO: do conversion

		while (rs.next()) {
			System.out.println(rs.getString("zh"));
		}
	}

}
