package com.sparrowrecsys.online.datamanager;

import com.sparrowrecsys.online.model.Embedding;
import com.sparrowrecsys.online.util.Config;
import com.sparrowrecsys.online.util.Utility;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.io.File;
import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * DataManager is an utility class, takes charge of all data loading logic.
 */

public class DataManager {
    //singleton instance
    private static volatile DataManager instance;
    HashMap<Integer, Movie> movieMap;
    HashMap<Integer, User> userMap;
    //genre reverse index for quick querying all movies in a genre
    HashMap<String, List<Movie>> genreReverseIndexMap;

    private int currentMaxMovieId = 0;
    private int currentMaxUserId = 0;

    private DataManager(){
        this.movieMap = new HashMap<>();
        this.userMap = new HashMap<>();
        this.genreReverseIndexMap = new HashMap<>();
        instance = this;
    }

    public static DataManager getInstance(){
        if (null == instance){
            synchronized (DataManager.class){
                if (null == instance){
                    instance = new DataManager();
                }
            }
        }
        return instance;
    }

    //load data from file system including movie, rating, link data and model data like embedding vectors.
    public void loadData(String movieDataPath, String linkDataPath, String ratingDataPath, String movieEmbPath, String userEmbPath, String movieRedisKey, String userRedisKey) throws Exception{
        Connection conn = DatabaseManager.getDBConnection();
        if (!DatabaseManager.loadMoviesFromDB(conn, this)) {
            loadMovieData(movieDataPath);
        }
        if (!DatabaseManager.loadRatingsFromDB(conn, this)) {
            loadRatingData(ratingDataPath);
        }
        DatabaseManager.closeDBConnection(conn);

        loadLinkData(linkDataPath);

        // Below features will be from online services instead of files
//        loadMovieEmb(movieEmbPath, movieRedisKey);
//        if (Config.IS_LOAD_ITEM_FEATURE_FROM_REDIS){
//            loadMovieFeatures("mf:");
//        }
//
//        loadUserEmb(userEmbPath, userRedisKey);

        currentMaxMovieId = this.movieMap.keySet().stream().mapToInt(v -> v).max().orElse(0);
        currentMaxUserId = this.userMap.keySet().stream().mapToInt(v -> v).max().orElse(0);
    }

    //load movie data from movies.csv
    private void loadMovieData(String movieDataPath) throws Exception{
        System.out.println("Loading movie data from " + movieDataPath + " ...");
        boolean skipFirstLine = true;
        try (Scanner scanner = new Scanner(new File(movieDataPath))) {
            while (scanner.hasNextLine()) {
                String movieRawData = scanner.nextLine();
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                String[] movieData = movieRawData.split(",");
                if (movieData.length == 3){
                    Movie movie = new Movie();
                    movie.setMovieId(Integer.parseInt(movieData[0]));
                    int releaseYear = parseReleaseYear(movieData[1].trim());
                    if (releaseYear == -1){
                        movie.setTitle(movieData[1].trim());
                    }else{
                        movie.setReleaseYear(releaseYear);
                        movie.setTitle(movieData[1].trim().substring(0, movieData[1].trim().length()-6).trim());
                    }
                    String genres = movieData[2];
                    if (!genres.trim().isEmpty()){
                        String[] genreArray = genres.split("\\|");
                        for (String genre : genreArray){
                            movie.addGenre(genre);
                            addMovie2GenreIndex(genre, movie);
                        }
                    }
                    this.movieMap.put(movie.getMovieId(), movie);
                }
            }
        }
        System.out.println("Loading movie data completed. " + this.movieMap.size() + " movies in total.");
    }

    //load movie embedding
    private void loadMovieEmb(String movieEmbPath, String embKey) throws Exception{
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_FILE)) {
            System.out.println("Loading movie embedding from " + movieEmbPath + " ...");
            int validEmbCount = 0;
            try (Scanner scanner = new Scanner(new File(movieEmbPath))) {
                while (scanner.hasNextLine()) {
                    String movieRawEmbData = scanner.nextLine();
                    String[] movieEmbData = movieRawEmbData.split(":");
                    if (movieEmbData.length == 2) {
                        Movie m = getMovieById(Integer.parseInt(movieEmbData[0]));
                        if (null == m) {
                            continue;
                        }
                        m.setEmb(Utility.parseEmbStr(movieEmbData[1]));
                        validEmbCount++;
                    }
                }
            }
            System.out.println("Loading movie embedding completed. " + validEmbCount + " movie embeddings in total.");
        }else{
            System.out.println("Loading movie embedding from Redis ...");
            Set<String> movieEmbKeys = RedisClient.getInstance().keys(embKey + "*");
            int validEmbCount = 0;
            for (String movieEmbKey : movieEmbKeys){
                String movieId = movieEmbKey.split(":")[1];
                Movie m = getMovieById(Integer.parseInt(movieId));
                if (null == m) {
                    continue;
                }
                m.setEmb(Utility.parseEmbStr(RedisClient.getInstance().get(movieEmbKey)));
                validEmbCount++;
            }
            System.out.println("Loading movie embedding completed. " + validEmbCount + " movie embeddings in total.");
        }
    }

    //load movie features
    private void loadMovieFeatures(String movieFeaturesPrefix) throws Exception{
        System.out.println("Loading movie features from Redis ...");
        Set<String> movieFeaturesKeys = RedisClient.getInstance().keys(movieFeaturesPrefix + "*");
        int validFeaturesCount = 0;
        for (String movieFeaturesKey : movieFeaturesKeys){
            String movieId = movieFeaturesKey.split(":")[1];
            Movie m = getMovieById(Integer.parseInt(movieId));
            if (null == m) {
                continue;
            }
            m.setMovieFeatures(RedisClient.getInstance().hgetAll(movieFeaturesKey));
            validFeaturesCount++;
        }
        System.out.println("Loading movie features completed. " + validFeaturesCount + " movie features in total.");
    }

    //load user embedding
    private void loadUserEmb(String userEmbPath, String embKey) throws Exception{
        if (Config.EMB_DATA_SOURCE.equals(Config.DATA_SOURCE_FILE)) {
            System.out.println("Loading user embedding from " + userEmbPath + " ...");
            int validEmbCount = 0;
            try (Scanner scanner = new Scanner(new File(userEmbPath))) {
                while (scanner.hasNextLine()) {
                    String userRawEmbData = scanner.nextLine();
                    String[] userEmbData = userRawEmbData.split(":");
                    if (userEmbData.length == 2) {
                        User u = getUserById(Integer.parseInt(userEmbData[0]));
                        if (null == u) {
                            continue;
                        }
                        u.setEmb(Utility.parseEmbStr(userEmbData[1]));
                        validEmbCount++;
                    }
                }
            }
            System.out.println("Loading user embedding completed. " + validEmbCount + " user embeddings in total.");
        }
    }

    //parse release year
    protected int parseReleaseYear(String rawTitle){
        if (null == rawTitle || rawTitle.trim().length() < 6){
            return -1;
        }else{
            String yearString = rawTitle.trim().substring(rawTitle.length()-5, rawTitle.length()-1);
            try{
                return Integer.parseInt(yearString);
            }catch (NumberFormatException exception){
                return -1;
            }
        }
    }

    //load links data from links.csv
    private void loadLinkData(String linkDataPath) throws Exception{
        System.out.println("Loading link data from " + linkDataPath + " ...");
        int count = 0;
        boolean skipFirstLine = true;
        try (Scanner scanner = new Scanner(new File(linkDataPath))) {
            while (scanner.hasNextLine()) {
                String linkRawData = scanner.nextLine();
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                String[] linkData = linkRawData.split(",");
                if (linkData.length == 3){
                    int movieId = Integer.parseInt(linkData[0]);
                    Movie movie = this.movieMap.get(movieId);
                    if (null != movie){
                        count++;
                        movie.setImdbId(linkData[1].trim());
                        movie.setTmdbId(linkData[2].trim());
                    }
                }
            }
        }
        System.out.println("Loading link data completed. " + count + " links in total.");
    }

    //load ratings data from ratings.csv
    private void loadRatingData(String ratingDataPath) throws Exception{
        System.out.println("Loading rating data from " + ratingDataPath + " ...");
        boolean skipFirstLine = true;
        int count = 0;
        try (Scanner scanner = new Scanner(new File(ratingDataPath))) {
            while (scanner.hasNextLine()) {
                String ratingRawData = scanner.nextLine();
                if (skipFirstLine){
                    skipFirstLine = false;
                    continue;
                }
                String[] linkData = ratingRawData.split(",");
                if (linkData.length == 4){
                    count ++;
                    Rating rating = new Rating();
                    rating.setUserId(Integer.parseInt(linkData[0]));
                    rating.setMovieId(Integer.parseInt(linkData[1]));
                    rating.setScore(Float.parseFloat(linkData[2]));
                    rating.setTimestamp(Long.parseLong(linkData[3]));
                    Movie movie = this.movieMap.get(rating.getMovieId());
                    if (null != movie){
                        movie.addRating(rating);
                    }
                    if (!this.userMap.containsKey(rating.getUserId())){
                        User user = new User();
                        user.setUserId(rating.getUserId());
                        this.userMap.put(user.getUserId(), user);
                    }
                    this.userMap.get(rating.getUserId()).addRating(rating);
                }
            }
        }

        System.out.println("Loading rating data completed. " + count + " ratings in total.");
    }

    //add movie to genre reversed index
    protected void addMovie2GenreIndex(String genre, Movie movie){
        if (!this.genreReverseIndexMap.containsKey(genre)){
            this.genreReverseIndexMap.put(genre, new ArrayList<>());
        }
        this.genreReverseIndexMap.get(genre).add(movie);
    }

    //get movies by genre, and order the movies by sortBy method
    public List<Movie> getMoviesByGenre(String genre, int size, String sortBy){
        if (null != genre){
            List<Movie> movies = new ArrayList<>(this.genreReverseIndexMap.get(genre));
            switch (sortBy){
                case "rating":movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));break;
                case "releaseYear": movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));break;
                default:
            }

            if (movies.size() > size){
                return movies.subList(0, size);
            }
            return movies;
        }
        return null;
    }

    //get top N movies order by sortBy method
    public List<Movie> getMovies(int size, String sortBy){
            List<Movie> movies = new ArrayList<>(movieMap.values());
            switch (sortBy){
                case "rating":movies.sort((m1, m2) -> Double.compare(m2.getAverageRating(), m1.getAverageRating()));break;
                case "releaseYear": movies.sort((m1, m2) -> Integer.compare(m2.getReleaseYear(), m1.getReleaseYear()));break;
                default:
            }

            if (movies.size() > size){
                return movies.subList(0, size);
            }
            return movies;
    }

    //get movie object by movie id
    public Movie getMovieById(int movieId){
        return this.movieMap.get(movieId);
    }

    //get user object by user id
    public User getUserById(int userId){
        return this.userMap.get(userId);
    }

    public int addNewMovie(String title, String genres) {
        int movieId = ++currentMaxMovieId;

        // save to DB
        Connection conn = DatabaseManager.getDBConnection();
        DatabaseManager.insertNewMovie(conn, (long) movieId, title, genres);
        DatabaseManager.closeDBConnection(conn);

        // save to memory
        Movie movie = new Movie();
        movie.setMovieId(movieId);
        int releaseYear = parseReleaseYear(title.trim());
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
                addMovie2GenreIndex(genre, movie);
            }
        }
        this.movieMap.put(movie.getMovieId(), movie);

        // send kafka message
        KafkaMessaging.sendNewMovie(movie);

        return movieId;
    }

    public Rating addNewRating(int userId, int movieId, float score) {
        long time = System.currentTimeMillis() / 1000;

        // save to DB
        Connection conn = DatabaseManager.getDBConnection();
        DatabaseManager.insertNewRating(conn, (long) userId, (long) movieId, score, time);
        DatabaseManager.closeDBConnection(conn);

        // save to memory
        Rating rating = new Rating();
        rating.setUserId(userId);
        rating.setMovieId(movieId);
        rating.setScore(score);
        rating.setTimestamp(time);
        Movie movie = this.movieMap.get(rating.getMovieId());
        if (null != movie){
            movie.addRating(rating);
        }

        // add user if needed
        if (!this.userMap.containsKey(rating.getUserId())){
            User user = new User();
            user.setUserId(rating.getUserId());
            this.userMap.put(user.getUserId(), user);
        }

        this.userMap.get(rating.getUserId()).addRating(rating);

        // send kafka message
        KafkaMessaging.sendNewRating(rating);

        return rating;
    }

    public List<Movie> retrievalMoviesByANN(Movie movie) {
        List<Movie> ret = Collections.emptyList();

        Jedis redis = new Jedis(Config.REDIS_SERVER, Config.REDIS_PORT);

        int movieId = movie.getMovieId();

        // get movie embedding from redis
        String movieEmbStr = getRedisValueWithVersionKey(
                redis,
                Config.REDIS_KEY_MOVIE_EMBEDDING_VERSION,
                Config.REDIS_KEY_PREFIX_MOVIE_EMBEDDING,
                String.valueOf(movieId)
        );

        // check redis embedding
        if (movieEmbStr != null && !movieEmbStr.isEmpty()) {
            movie.setEmb(new Embedding(
                    Arrays.stream(movieEmbStr.split(",")).map(Float::valueOf)
                            .collect(Collectors.toCollection(ArrayList::new))));
            System.out.println("Set movie embeddings from redis, movieId: " + movieId);

            ret = retrievalMoviesByANN(redis, movieId);
        }

        redis.close();
        return ret;
    }

    private List<Movie> retrievalMoviesByANN(Jedis redis, int movieId) {
        System.out.println("Get related movies for movieId: " + movieId);

        // get movie buckets from redis
        String bucketIdsStr = getRedisValueWithVersionKey(
                redis,
                Config.REDIS_KEY_LSH_MOVIE_BUCKETS_VERSION,
                Config.REDIS_KEY_PREFIX_LSH_MOVIE_BUCKETS,
                String.valueOf(movieId)
        );
        System.out.println("Movie buckets: " + bucketIdsStr);

        // get bucket movies from redis
        return getBucketMoviesFromRedis(redis, bucketIdsStr)
                .stream()
                .filter(m -> m.getMovieId() != movieId) // remove current movie
                .collect(Collectors.toList());
    }

    public List<Movie> retrievalMoviesByANN(User user) {
        System.out.println("Get related movies for userId: " + user.getUserId());

        List<Movie> ret = Collections.emptyList();

        Jedis redis = new Jedis(Config.REDIS_SERVER, Config.REDIS_PORT);

        int userId = user.getUserId();

        // get user embedding from redis
        String userEmbStr = getRedisValueWithVersionKey(
                redis,
                Config.REDIS_KEY_USER_EMBEDDING_VERSION,
                Config.REDIS_KEY_PREFIX_USER_EMBEDDING,
                String.valueOf(userId)
        );

        // check redis embedding
        if (userEmbStr != null && !userEmbStr.isEmpty()) {
            user.setEmb(new Embedding(
                    Arrays.stream(userEmbStr.split("\\|")[1].split(",")).map(Float::valueOf)
                            .collect(Collectors.toCollection(ArrayList::new))));
            System.out.println("Set user embeddings from redis, userId: " + userId);
        }

        // get user buckets
        String bucketIdsStr = getRedisValueWithVersionKey(
                redis,
                Config.REDIS_KEY_LSH_USER_BUCKETS_VERSION,
                Config.REDIS_KEY_PREFIX_LSH_USER_BUCKETS,
                String.valueOf(userId)
        );
        System.out.println("User buckets: " + bucketIdsStr);

        // get bucket movies
        ret = getBucketMoviesFromRedis(redis, bucketIdsStr);

        // check empty
        if (ret.isEmpty()) {

            // retrieve by recently rated movies
            String recentMovieIdStr =
                    redis.hget(Config.REDIS_KEY_PREFIX_USER_FEATURE + ":" + userId, "userRatedMovie1");

            if (recentMovieIdStr != null && !recentMovieIdStr.isEmpty()) {
                System.out.println("Get related movies for user with recently rated movie: " + recentMovieIdStr);

                int recentMovieId = Integer.parseInt(recentMovieIdStr);
                ret = retrievalMoviesByANN(redis, recentMovieId);
            }
        }

        redis.close();
        return ret;
    }

    private List<Movie> getBucketMoviesFromRedis(Jedis redis, String bucketIdsStr) {

        // check redis buckets
        if (bucketIdsStr != null && !bucketIdsStr.isEmpty()) {
            // get related movies from buckets in redis
            List<Integer> relatedMovieIds = Arrays.stream(bucketIdsStr.split(","))
                    .map(bucketId -> getRedisValueWithVersionKey(
                            redis,
                            Config.REDIS_KEY_LSH_BUCKET_MOVIES_VERSION,
                            Config.REDIS_KEY_PREFIX_LSH_BUCKET_MOVIES,
                            bucketId
                    ))
                    .flatMap(movieIdsStr -> {
                        if (movieIdsStr == null || movieIdsStr.isEmpty()) {
                            return Stream.empty();
                        } else {
                            return Arrays.stream(movieIdsStr.split(",")).map(Integer::valueOf);
                        }
                    })
                    .distinct()
                    .collect(Collectors.toList());

            List<Movie> relatedMovies = new ArrayList<>();
            relatedMovieIds.forEach(id -> {
                if (this.movieMap.containsKey(id)) {
                    relatedMovies.add(this.movieMap.get(id));
                }
            });

            // get embeddings from redis
            String embeddingVersion = redis.get(Config.REDIS_KEY_MOVIE_EMBEDDING_VERSION);
            List<String> relatedMovieEmbeddings = redis.mget(relatedMovies.stream()
                    .map(
                            v -> Config.REDIS_KEY_PREFIX_MOVIE_EMBEDDING
                                    + ":" + embeddingVersion + ":" + v.getMovieId()
                    ).toArray(String[]::new));

            // set movie embeddings
            for (int i = 0; i < relatedMovies.size(); i++) {
                Movie m = relatedMovies.get(i);
                String embeddingStr = relatedMovieEmbeddings.get(i);

                if (embeddingStr != null && !embeddingStr.isEmpty()) {
                    m.setEmb(new Embedding(
                            Arrays.stream(embeddingStr.split(",")).map(Float::valueOf)
                                    .collect(Collectors.toCollection(ArrayList::new))));
                }
            }

            System.out.println("Get related movies from buckets, count: " + relatedMovies.size());

            return relatedMovies;
        }

        return Collections.emptyList();
    }

    private String getRedisValueWithVersionKey(Jedis redis, String versionKey, String keyPrefix, String id) {

        String version = redis.get(versionKey);
        if (version == null || version.isEmpty()) {
            return null;
        }

        String key = keyPrefix + ":" + version + ":" + id;

        return redis.get(key);
    }

    public void getMovieFeaturesFromRedis(List<Movie> movies) {

        System.out.println("Get movie features, movie count: " + movies.size());

        Jedis redis = new Jedis(Config.REDIS_SERVER, Config.REDIS_PORT);

        Pipeline pipeline = redis.pipelined();
        List<Response<Map<String, String>>> responses = new ArrayList<>();

        for (Movie movie : movies) {
            Response<Map<String, String>> resp =
                    pipeline.hgetAll(Config.REDIS_KEY_PREFIX_MOVIE_FEATURE + ":" + movie.getMovieId());
            responses.add(resp);
        }

        pipeline.sync();

        for (int i = 0; i < responses.size(); i++) {
            if (responses.get(i).get() != null) {
                movies.get(i).setMovieFeatures(responses.get(i).get());
            }
        }

        redis.close();
    }
}
