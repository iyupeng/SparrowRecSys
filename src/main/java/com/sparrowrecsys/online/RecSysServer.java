package com.sparrowrecsys.online;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.service.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/***
 * Recsys Server, end point of online recommendation service
 */

public class RecSysServer {

    public static void main(String[] args) throws Exception {
        new RecSysServer().run();
    }

    //recsys server port number
    private static final int DEFAULT_PORT = 8010;

    public void run() throws Exception{

        int port = DEFAULT_PORT;
        try {
            port = Integer.parseInt(System.getenv("PORT"));
        } catch (NumberFormatException ignored) {}

        //set ip and port number
        InetSocketAddress inetAddress = new InetSocketAddress("0.0.0.0", port);
        Server server = new Server(inetAddress);

        //get index.html path
        URL webRootLocation = this.getClass().getResource("/webroot/index.html");
        if (webRootLocation == null)
        {
            throw new IllegalStateException("Unable to determine webroot URL location");
        }
        String basePath = webRootLocation.toExternalForm().replaceFirst("/index.html$","/");

        //set index.html as the root page
        URI webRootUri = URI.create(webRootLocation.toExternalForm().replaceFirst("/index.html$","/"));
        System.out.printf("Web Root URI: %s%n", basePath);

        //load all the data to DataManager
        DataManager.getInstance().loadData(
                resourceToLocal("/webroot/sampledata/movies.csv"),
                resourceToLocal("/webroot/sampledata/links.csv"),
                resourceToLocal("/webroot/sampledata/ratings.csv"),
                resourceToLocal("/webroot/modeldata/item2vecEmb.csv"),
                resourceToLocal("/webroot/modeldata/userEmb.csv"),
                "i2vEmb",
                "uEmb");

        //create server context
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
//        context.setBaseResource(Resource.newResource(webRootUri));
        context.setResourceBase(basePath);
        context.setWelcomeFiles(new String[] { "index.html" });
        context.getMimeTypes().addMimeMapping("txt","text/plain;charset=utf-8");

        //bind services with different servlets
        context.addServlet(DefaultServlet.class,"/");
        context.addServlet(new ServletHolder(new MovieService()), "/getmovie");
        context.addServlet(new ServletHolder(new UserService()), "/getuser");
        context.addServlet(new ServletHolder(new SimilarMovieService()), "/getsimilarmovie");
        context.addServlet(new ServletHolder(new RecommendationService()), "/getrecommendation");
        context.addServlet(new ServletHolder(new RecForYouService()), "/getrecforyou");
        context.addServlet(new ServletHolder(new MovieCreateService()), "/createmovie");
        context.addServlet(new ServletHolder(new RatingCreateService()), "/createrating");

        //set url handler
        server.setHandler(context);
        System.out.println("RecSys Server has started.");

        //start Server
        server.start();
        server.join();
    }

    public static String resourceToLocal(String resourcePath) throws IOException {
        Path outPath = Paths.get("/tmp/" + resourcePath);
        InputStream resourceFileStream = RecSysServer.class.getResourceAsStream(resourcePath);
        Files.createDirectories(outPath.getParent());
        Files.copy(resourceFileStream, outPath, StandardCopyOption.REPLACE_EXISTING);
        resourceFileStream.close();
        String ret = outPath.toUri().getPath();
        System.out.println(ret);
        return ret;
    }
}
