package com.sparrowrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MovieCreateService, create new movie
 */

public class MovieCreateService extends HttpServlet {
    public static String[] ALL_GENRES = {
            "Film-Noir", "Action", "Adventure", "Horror", "Romance", "War", "Comedy", "Western", "Documentary",
            "Sci-Fi", "Drama", "Thriller",
            "Crime", "Fantasy", "Animation", "IMAX", "Mystery", "Children", "Musical"};

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            System.out.println("create movie request: " + request.getParameterMap().toString());

            //get movie id via url parameter
            String title = request.getParameter("title").trim().replace('\t', ' ');
            Integer[] genreIds = Arrays.stream(request.getParameter("genres").trim().split(",")).map(Integer::parseInt).toArray(Integer[]::new);
            List<String> genreNames = new ArrayList<>();
            for (Integer i : genreIds) {
                if (i >= 0 && i < ALL_GENRES.length) {
                    String genre = ALL_GENRES[i];
                    if (!genreNames.contains(genre)) {
                        genreNames.add(genre);
                    }
                }
            }
            String genres = String.join("|", genreNames);

            int movieId = DataManager.getInstance().addNewMovie(title, genres);

            //get movie object from DataManager
            Movie movie = DataManager.getInstance().getMovieById(movieId);

            //convert movie object to json format and return
            if (movie != null) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonMovie = mapper.writeValueAsString(movie);
                response.getWriter().println(jsonMovie);
            } else {
                response.getWriter().println("failed to add new movie.");
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.getWriter().println(e.getMessage());
        }
    }
}
