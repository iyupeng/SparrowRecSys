package com.sparrowrecsys.online.recprocess;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.User;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.datamanager.RedisClient;
import com.sparrowrecsys.online.util.Config;
import com.sparrowrecsys.online.util.Utility;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

import static com.sparrowrecsys.online.util.HttpClient.asyncSinglePostRequest;

/**
 * Recommendation process of similar movies
 */

public class RecForYouProcess {

    /**
     * get recommendation movie list
     * @param userId input user id
     * @param size  size of similar items
     * @param model model used for calculating similarity
     * @return  list of similar movies
     */
    public static List<Movie> getRecList(int userId, int size, String model){
        User user = DataManager.getInstance().getUserById(userId);
        if (null == user){
            return new ArrayList<>();
        }
        List<Movie> candidates = DataManager.getInstance().retrievalMoviesByANN(user);

        if (Config.IS_LOAD_USER_FEATURE_FROM_REDIS){
            String userFeaturesKey = Config.REDIS_KEY_PREFIX_USER_FEATURE + ":" + userId;
            Map<String, String> userFeatures = RedisClient.getInstance().hgetAll(userFeaturesKey);
            if (null != userFeatures){
                user.setUserFeatures(userFeatures);
            }
        }

        if (Config.IS_LOAD_ITEM_FEATURE_FROM_REDIS){
            DataManager.getInstance().getMovieFeaturesFromRedis(candidates);
        }

        List<Movie> rankedList = ranker(user, candidates, model);

        if (rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * rank candidates
     * @param user    input user
     * @param candidates    movie candidates
     * @param model     model name used for ranking
     * @return  ranked movie list
     */
    public static List<Movie> ranker(User user, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();

        switch (model){
            case "emb":
                for (Movie candidate : candidates){
                    double similarity = calculateEmbSimilarScore(user, candidate);
                    candidateScoreMap.put(candidate, similarity);
                }
                break;
            case "nerualcf":
                callNeuralCFTFServing(user, candidates, candidateScoreMap);
                break;
            case "wd":
                callWideDeepTFServing(user, candidates, candidateScoreMap);
                break;
            default:
                //default ranking in candidate set
                for (int i = 0 ; i < candidates.size(); i++){
                    candidateScoreMap.put(candidates.get(i), (double)(candidates.size() - i));
                }
        }

        List<Movie> rankedList = new ArrayList<>();
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));
        return rankedList;
    }

    /**
     * function to calculate similarity score based on embedding
     * @param user     input user
     * @param candidate candidate movie
     * @return  similarity score
     */
    public static double calculateEmbSimilarScore(User user, Movie candidate){
        if (null == user || null == candidate || null == user.getEmb()){
            return -1;
        }
        return user.getEmb().calculateSimilarity(candidate.getEmb());
    }

    /**
     * call TenserFlow serving to get the NeuralCF model inference result
     * @param user              input user
     * @param candidates        candidate movies
     * @param candidateScoreMap save prediction score into the score map
     */
    public static void callNeuralCFTFServing(User user, List<Movie> candidates, HashMap<Movie, Double> candidateScoreMap){
        if (null == user || null == candidates || candidates.size() == 0){
            return;
        }

        JSONArray instances = new JSONArray();
        for (Movie m : candidates){
            JSONObject instance = new JSONObject();
            instance.put("userId", user.getUserId());
            instance.put("movieId", m.getMovieId());
            instances.put(instance);
        }

        JSONObject instancesRoot = new JSONObject();
        instancesRoot.put("instances", instances);

        //need to confirm the tf serving end point
        String predictionScores = asyncSinglePostRequest("http://localhost:8501/v1/models/recmodel:predict", instancesRoot.toString());
        System.out.println("send user" + user.getUserId() + " request to tf serving.");

        JSONObject predictionsObject = new JSONObject(predictionScores);
        JSONArray scores = predictionsObject.getJSONArray("predictions");
        for (int i = 0 ; i < candidates.size(); i++){
            candidateScoreMap.put(candidates.get(i), scores.getJSONArray(i).getDouble(0));
        }
    }

    /**
     * call TenserFlow serving to get the NeuralCF model inference result
     * @param user              input user
     * @param candidates        candidate movies
     * @param candidateScoreMap save prediction score into the score map
     */
    public static void callWideDeepTFServing(User user, List<Movie> candidates, HashMap<Movie, Double> candidateScoreMap){
        if (null == user || null == candidates || candidates.size() == 0){
            return;
        }

        JSONArray instances = new JSONArray();
        for (Movie m : candidates){
            JSONObject instance = new JSONObject();

            instance.put("userId", user.getUserId());
            instance.put("userAvgRating", Float.valueOf(user.getUserFeature("userAvgRating", "0")));
            instance.put("userRatingStddev", Float.valueOf(user.getUserFeature("userRatingStddev", "0")));
            instance.put("userRatingCount", Integer.parseInt(user.getUserFeature("userRatingCount", "0")));
            instance.put("userRatedMovie1", Integer.parseInt(user.getUserFeature("userRatedMovie1", "0")));

            instance.put("userGenre1", user.getUserFeature("userGenre1", ""));
            instance.put("userGenre2", user.getUserFeature("userGenre2", ""));
            instance.put("userGenre3", user.getUserFeature("userGenre3", ""));
            instance.put("userGenre4", user.getUserFeature("userGenre4", ""));
            instance.put("userGenre5", user.getUserFeature("userGenre5", ""));

            instance.put("movieId", m.getMovieId());
            instance.put("releaseYear", Integer.parseInt(m.getMovieFeature("releaseYear", "1990")));
            instance.put("movieAvgRating", Float.valueOf(m.getMovieFeature("movieAvgRating", "0")));
            instance.put("movieRatingStddev", Float.valueOf(m.getMovieFeature("movieRatingStddev", "0")));
            instance.put("movieRatingCount", Integer.parseInt(m.getMovieFeature("movieRatingCount", "0")));

            instance.put("movieGenre1", m.getMovieFeature("movieGenre1", ""));
            instance.put("movieGenre2", m.getMovieFeature("movieGenre2", ""));
            instance.put("movieGenre3", m.getMovieFeature("movieGenre3", ""));

            instances.put(instance);
        }

        JSONObject instancesRoot = new JSONObject();
        instancesRoot.put("instances", instances);

        // get model version
        String modelVersion = "";
        String modelVersionStr = RedisClient.getInstance().get(Config.REDIS_KEY_VERSION_MODEL_WIDE_DEEP);
        if (modelVersionStr != null && !modelVersionStr.isEmpty()) {
            modelVersion = "/versions/" + modelVersionStr;

            System.out.println("Using wide&deep model version: " + modelVersionStr);
        }

        System.out.println("Send user and movie features to tf serving, first instance: ");
        System.out.println(instances.get(0).toString());

        //need to confirm the tf serving end point
        String predictionScores = asyncSinglePostRequest(
                Config.TF_SERVING_URL_PREFIX_WIDE_DEEP + modelVersion + ":predict",
                instancesRoot.toString()
        );

        JSONObject predictionsObject = new JSONObject(predictionScores);
        JSONArray scores = predictionsObject.getJSONArray("predictions");
        for (int i = 0 ; i < candidates.size(); i++){
            candidateScoreMap.put(candidates.get(i), scores.getJSONArray(i).getDouble(0));
        }
    }
}
