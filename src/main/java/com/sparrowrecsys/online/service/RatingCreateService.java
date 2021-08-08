package com.sparrowrecsys.online.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.datamanager.Rating;
import org.apache.parquet.Strings;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RatingCreateService, create new rating
 */

public class RatingCreateService extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setCharacterEncoding("UTF-8");
            response.setHeader("Access-Control-Allow-Origin", "*");

            System.out.println("create rating request: " + request.getParameterMap().toString());

            //get movie id via url parameter
            int userId = Integer.parseInt(request.getParameter("userId").trim());
            int movieId = Integer.parseInt(request.getParameter("movieId").trim());
            float rating = Float.parseFloat(request.getParameter("rating").trim());

            if (rating < 0 || rating > 5.0) {
                throw new InvalidParameterException("invalid rating score, should be in [0, 5].");
            }

            Rating r = DataManager.getInstance().addNewRating(userId, movieId, rating);

            //convert movie object to json format and return
            if (r != null) {
                ObjectMapper mapper = new ObjectMapper();
                String jsonMovie = mapper.writeValueAsString(r);
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
