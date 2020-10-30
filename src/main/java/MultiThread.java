import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedAbstractActor;
import akka.routing.RoundRobinPool;
import com.vk.api.sdk.objects.newsfeed.responses.SearchResponse;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import java.util.List;

public class MultiThread {

    boolean finish = false;
    public static Set<String> queries;
    static Map<String, Map<Integer, List<SearchResponse>>> results;

    MultiThread(Set<String> queries, List<Integer> userIds) throws IOException, OAuthProblemException, OAuthSystemException {
        results = new TreeMap<>();
        this.queries = queries;

        final ActorSystem system = ActorSystem.create("MySystem");
        ActorRef master = system.actorOf(MasterActor.props(), "master");

        master.tell(new Start(queries, userIds), null);

        while (!master.isTerminated()) {
            finish = false;
        }

        System.out.println("work is finished");
        finish = true;
    }


    public static class DownloaderActor extends UntypedAbstractActor {

        public static Props props() {
            return Props.create(DownloaderActor.class);
        }

        @Override
        public void onReceive(Object message) throws Throwable {
            Class<?> cls = message.getClass();

            Field queryField = cls.getField("query");
            Field userIdField = cls.getField("userNumber");

            String query = (String) queryField.get(message);
            String userId = "user" + (int) userIdField.get(message);

            NewsParser newsParser = new NewsParser(userId);
            //статистические данные
            Map<Integer, List<SearchResponse>> answer = newsParser.parseByDays(query, 5);

            results.put(query, answer);

            sender().tell(results, self());

        }
    }


    public static class MasterActor extends UntypedAbstractActor {


        final ActorRef worker = getContext().actorOf((new RoundRobinPool(5))
                .props(DownloaderActor.props()), "downloader");

        public static Props props() {
            return Props.create(MasterActor.class);
        }

        CommentsCounter counter;

        public void onReceive(Object message) throws Throwable {

            //   System.out.println(message.getClass());
            if (message.getClass() == MultiThread.Start.class) {
                counter = new CommentsCounter(0);
                Class<?> cls = message.getClass();

                Field queriesList = cls.getField("queries");
                Field userNumberField = cls.getField("userNumber");
                Set<String> queries = (Set<String>) queriesList.get(message);
                List<Integer> userIds = (List<Integer>) userNumberField.get(message);


                //раздаем запросы воркерам
                int i = 0;
                for (String query : queries) {
                    worker.tell(new Help(query, userIds.get(i)), self());
                    i++;
                }

            } else if (message.getClass() == TreeMap.class) {
                counter.append();

                if (counter.getCount() == MultiThread.queries.size()) {
                    System.out.println(counter.getCount());
                    System.out.println("ready");
                    context().system().terminate();
                }

            }

        }

    }

    public static class Start {
        public List<Integer> userNumber;
        public Set<String> queries;

        Start(Set<String> queries, List<Integer> userNumber)

        {
            this.userNumber = userNumber;
            this.queries = queries;
        }
    }

    public static class Help {
        public int userNumber;
        public String query;

        Help(String query, int userNumber)

        {
            this.userNumber = userNumber;
            this.query = query;
        }
    }

    public static class CommentsCounter {
        private int count;

        CommentsCounter(int count) {
            this.count = count;
        }

        public void append() {
            this.count += 1;
        }

        public int getCount() {
            return this.count;
        }
    }
}
