package no.unit.instservice;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class CleanUp {

    private static final String DATABASE_URI = "jdbc:postgresql://instservice-test.bibsys.no/instreg";
    private static final String USER = "postgres";
    private static final String PASSWORD = "institution";
    private static final String LINK_SELECT = "SELECT * FROM unitrelations;";
    private static final String DELETE_LINK = "DELETE FROM unitrelations WHERE fromid = ? and toid = ?;";
    private static final String DELETE_LINK_TEST = "DELETE FROM unitrelations WHERE fromid = 1144 and toid = 1145;";
    private static final String UNIT_SELECT = "SELECT * FROM units WHERE ";

    private static void deleteDuplicateLinks() throws SQLException, IOException {
        try (Connection connection =
                DriverManager
                        .getConnection(
                                String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {

                Files.lines(Paths.get("e:\\instservice\\dublette_relasjoner_slettes.txt"))
                        .map(line -> line.split(";")[0]).forEach(links -> {
                            System.out.println(links);

                            int fromId = Integer.parseInt(links.split(",")[0]);
                            int toId = Integer.parseInt(links.split(",")[1]);

                            System.out.printf("%d - %d%n", fromId, toId);

                            PreparedStatement statement;
                            try {
                                statement = connection.prepareStatement(DELETE_LINK);
                                statement.setInt(1, fromId);
                                statement.setInt(2, toId);
                                statement.execute();

                                statement =
                                        connection.prepareStatement(
                                                "INSERT INTO unitrelations(fromid,toid) VALUES (?,?)");
                                statement.setInt(1, fromId);
                                statement.setInt(2, toId);
                                statement.execute();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        });
            }
        }

    }

    private static void addActive() throws SQLException, IOException {

        final String ATTRIBUTE_ADD =
                "INSERT INTO public.attributes(" +
                        "    unitid, value, attributetypeid)" +
                        "    VALUES (?, ?, ?);";

        List<String> instIdList =
                Files.lines(Paths.get("e:\\instservice\\institusjoner.txt")).map(lines -> lines.split(";")[0])
                        .collect(Collectors.toList());

        // System.out.println(instId);

        instIdList.forEach(institution -> {

            System.out.println(institution);

            try (Connection connection =
                    DriverManager
                            .getConnection(
                                    String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
                {
                    PreparedStatement statement = connection.prepareStatement(ATTRIBUTE_ADD);
                    statement.setInt(1, Integer.parseInt(institution));
                    statement.setString(2, "yes");
                    statement.setInt(3, 35);
                    statement.execute();
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

    private static void deleteLinks() throws IOException, SQLException {
        final String DELETE_LINKS = "DELETE FROM unitrelations WHERE fromId = ? AND toid = ?";
        try (Connection connection =
                DriverManager
                        .getConnection(
                                String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {

                Files.lines(Paths.get("e:\\instservice\\relasjoner_slettes.txt"))
                        .forEach(line -> {
                            String links = line.split(";")[0];
                            int deleteFrom = Integer.parseInt(links.split(",")[0]);
                            int deleteTo = Integer.parseInt(links.split(",")[1]);
                            System.out.printf("%d - %d%n", deleteFrom, deleteTo);

                            PreparedStatement statement;
                            try {

                                statement = connection.prepareStatement(DELETE_LINKS);
                                statement.setInt(1, deleteFrom);
                                statement.setInt(2, deleteTo);
                                statement.execute();

                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        });
            }
        }

    }

    private static void moveLinks() throws IOException, SQLException {
        final String DELETE_LINKS = "DELETE FROM unitrelations WHERE fromId = ? AND toid = ?";
        final String ADD_LINKS = "INSERT INTO unitrelations(fromid,toid) VALUES (?,?)";
        try (Connection connection =
                DriverManager
                        .getConnection(
                                String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {

                Files.lines(Paths.get("e:\\instservice\\relasjoner_flyttes.txt"))
                        .forEach(line -> {
                            String links = line.split(";")[0];
                            int deleteFrom = Integer.parseInt(links.split("->")[0].split(",")[0]);
                            int deleteTo = Integer.parseInt(links.split("->")[0].split(",")[1]);
                            System.out.printf("%d - %d%n", deleteFrom, deleteTo);
                            int addFrom = Integer.parseInt(links.split("->")[1].split(",")[0]);
                            int addTo = Integer.parseInt(links.split("->")[1].split(",")[1]);
                            System.out.printf("%d - %d%n", addFrom, addTo);

                            PreparedStatement statement;
                            try {

                                statement = connection.prepareStatement(DELETE_LINKS);
                                statement.setInt(1, deleteFrom);
                                statement.setInt(2, deleteTo);
                                statement.execute();

                                statement = connection.prepareStatement(ADD_LINKS);
                                statement.setInt(1, addFrom);
                                statement.setInt(2, addTo);
                                statement.execute();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        });
            }
        }

    }

    private static void deleteUnits() throws SQLException, IOException {

        final String DELETE_UNIT = "DELETE FROM units WHERE id = ?";
        final String DELETE_LINKS = "DELETE FROM unitrelations WHERE fromId = ? OR toid = ?";
        final String DELETE_ATTRIBUTES = "DELETE FROM attributes WHERE unitid = ?";
        try (Connection connection =
                DriverManager
                        .getConnection(
                                String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {

                Files.lines(Paths.get("e:\\instservice\\enheter_slettes.txt"))
                        .forEach(line -> {
                            int id = Integer.parseInt(line.split(";")[0]);
                            System.out.println(id);

                            PreparedStatement statement;
                            try {
                                statement = connection.prepareStatement(DELETE_UNIT);
                                statement.setInt(1, id);
                                statement.execute();

                                statement = connection.prepareStatement(DELETE_ATTRIBUTES);
                                statement.setInt(1, id);
                                statement.execute();

                                statement = connection.prepareStatement(DELETE_LINKS);
                                statement.setInt(1, id);
                                statement.setInt(2, id);
                                statement.execute();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        });
            }
        }
    }

    private static void changeLibraryNames() throws SQLException, IOException {

        final String UPDATE_DESCRIPTION = "UPDATE public.units SET description=? WHERE id = ?;";

        try (Connection connection =
                DriverManager
                        .getConnection(
                                String.format("%s?user=%s&password=%s", DATABASE_URI, USER, PASSWORD))) {
            {

                Files.lines(Paths.get("e:\\instservice\\endre_description.txt"))
                        .forEach(line -> {
                            String[] splits = line.split((";"));
                            int id = Integer.parseInt(splits[0]);
                            String description = splits[2];

                            System.out.printf("%d - %s%n", id, description);

                            PreparedStatement statement;
                            try {
                                statement = connection.prepareStatement(UPDATE_DESCRIPTION);
                                statement.setInt(2, id);
                                statement.setString(1, description);

                                statement.execute();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        });
            }
        }
    }

    public static void main(String... args) throws SQLException, IOException {
        // CleanUp.addActive();
        // CleanUp.deleteDuplicateLinks();
        // CleanUp.changeLibraryNames();
        // CleanUp.deleteUnits();
        // CleanUp.moveLinks();
        CleanUp.deleteLinks();
    }
}
