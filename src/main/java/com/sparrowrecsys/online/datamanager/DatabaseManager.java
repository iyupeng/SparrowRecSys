package com.sparrowrecsys.online.datamanager;

import com.sparrowrecsys.online.util.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * MySQL operations for data stored in DB.
 */
public class DatabaseManager {
    static {
        System.out.println("Loading driver...");

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            System.out.println("Driver loaded!");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot find the driver in the classpath!", e);
        }
    }

    public static Connection getDBConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(Config.MYSQL_DB_CONNECTION_URL);

        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }

        return conn;
    }

    public static void closeDBConnection(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
    }

    public static boolean loadMoviesFromDB(Connection conn, DataManager manager) {
        if (conn == null) {
            return false;
        }

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM movies");

            // Now do something with the ResultSet ....
            int i = 0;
            while (rs.next()) {
                Long movieId = rs.getLong(1);
                String title = rs.getString(2);
                String genres = rs.getString(3);
                if (i++ < 10) {
                    System.out.printf("%d, %s, %s%n", movieId, title, genres);
                }
                Movie movie = new Movie();
                movie.setMovieId(movieId.intValue());
                int releaseYear = manager.parseReleaseYear(title.trim());
                if (releaseYear == -1){
                    movie.setTitle(title.trim());
                }else{
                    movie.setReleaseYear(releaseYear);
                    movie.setTitle(title.trim().substring(0, title.trim().length()-6).trim());
                }
                if (!genres.trim().isEmpty()){
                    String[] genreArray = genres.split("\\|");
                    for (String genre : genreArray){
                        movie.addGenre(genre);
                        manager.addMovie2GenreIndex(genre, movie);
                    }
                }
                manager.movieMap.put(movie.getMovieId(), movie);
            }
            System.out.println("Loading movie data from DB completed. " + manager.movieMap.size() + " movies in total.");
            return true;
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            // it is a good idea to release
            // resources in a finally{} block
            // in reverse-order of their creation
            // if they are no-longer needed

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore

                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore

                stmt = null;
            }
        }

        return false;
    }

    public static boolean insertNewMovie(Connection conn, Long id, String title, String genres) {
        if (conn == null) {
            return false;
        }
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(
                    "INSERT INTO movies (movieId, title, genres) VALUES (?, ?, ?)");

            preparedStatement.setLong(1, id);
            preparedStatement.setString(2, title);
            preparedStatement.setString(3, genres);

            int res = preparedStatement.executeUpdate();
            return res > 0;
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException ex) {
                } // ignore
                preparedStatement = null;
            }
        }

        return false;
    }

    public static boolean loadRatingsFromDB(Connection conn, DataManager manager) {
        if (conn == null) {
            return false;
        }

        Statement stmt = null;
        ResultSet rs = null;

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM ratings");

            // Now do something with the ResultSet ....
            int count = 0;
            while (rs.next()) {
                count++;

                Long userId = rs.getLong(1);
                Long movieId = rs.getLong(2);
                float score = rs.getFloat(3);
                Long time = rs.getLong(4);
                if (count <= 10) {
                    System.out.printf("%d, %d, %f, %d%n", userId, movieId, score, time);
                }

                Rating rating = new Rating();
                rating.setUserId(userId.intValue());
                rating.setMovieId(movieId.intValue());
                rating.setScore(score);
                rating.setTimestamp(time);
                Movie movie = manager.movieMap.get(rating.getMovieId());
                if (null != movie){
                    movie.addRating(rating);
                }
                if (!manager.userMap.containsKey(rating.getUserId())){
                    User user = new User();
                    user.setUserId(rating.getUserId());
                    manager.userMap.put(user.getUserId(), user);
                }
                manager.userMap.get(rating.getUserId()).addRating(rating);
            }

            System.out.println("Loading rating data from DB completed. " + count + " ratings in total.");
            return true;
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            // it is a good idea to release
            // resources in a finally{} block
            // in reverse-order of their creation
            // if they are no-longer needed

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException sqlEx) {
                } // ignore

                rs = null;
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlEx) {
                } // ignore

                stmt = null;
            }
        }

        return false;
    }

    public static boolean insertNewRating(Connection conn, Long userId, Long movieId,
                                          float rating, Long time) {
        if (conn == null) {
            return false;
        }
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(
                    "INSERT INTO ratings (userId, movieId, rating, `timestamp`) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE rating=?, `timestamp`=?");

            preparedStatement.setLong(1, userId);
            preparedStatement.setLong(2, movieId);
            preparedStatement.setFloat(3, rating);
            preparedStatement.setLong(4, time);
            preparedStatement.setFloat(5, rating);
            preparedStatement.setLong(6, time);

            int res = preparedStatement.executeUpdate();
            return res > 0;
        } catch (SQLException ex) {
            // handle any errors
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        } finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException ex) {
                } // ignore
                preparedStatement = null;
            }
        }

        return false;
    }

}
