import com.vk.api.sdk.client.TransportClient;
import com.vk.api.sdk.client.VkApiClient;
import com.vk.api.sdk.client.actors.UserActor;
import com.vk.api.sdk.exceptions.ApiException;
import com.vk.api.sdk.exceptions.ClientException;
import com.vk.api.sdk.httpclient.HttpTransportClient;
import com.vk.api.sdk.objects.newsfeed.responses.SearchResponse;
import com.vk.api.sdk.queries.newsfeed.NewsfeedSearchQuery;
import org.apache.commons.collections4.list.TreeList;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NewsParser {

    public VkApiClient vkClient;
    public UserActor userActor;
    public static DataBase db;

    static {
        try {
            db = new DataBase();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String next_from;


    NewsParser(String userId) throws IOException, OAuthSystemException, OAuthProblemException {

        this.next_from = "";

        //получаем данные для аутентификации пользователя
        Properties props = new Properties();
        props.load(new FileInputStream(new File("src/main/resources/" + userId + ".properties")));
        User user = new User();
        user.setAPP_ID(Integer.valueOf(props.getProperty("app.id")));
        user.setCLIENT_SECRET(String.valueOf(props.getProperty("client.secret")));
        user.setACCESS_TOKEN(String.valueOf(props.getProperty("access_token")));
        createClient(user);
    }

    private void createClient(User user) {
        TransportClient transportClient = HttpTransportClient.getInstance();
        this.vkClient = new VkApiClient(transportClient);
        this.userActor = new UserActor(user.getAPP_ID(), user.getACCESS_TOKEN());
    }


    /**
     * Метод по извлечению ссылок с текста текущей новости
     * @param news - текущая новость
     * @return - список всех ссылок
     */
    static List<String> getLinks(String news) {
        List<String> links = new TreeList<String>();
        Pattern pattern = Pattern.compile("(http(s)?:\\/\\/)?(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-zA-Z0-9()]{2,6}\\b([-a-zA-Z0-9()@:%_\\+.~#?&//=]*)");
        Matcher matcher = pattern.matcher(news);

        while (matcher.find()) {
            int startIndex = matcher.start();
            int endIndex = matcher.end();
            String link = news.substring(startIndex, endIndex);
            links.add(link);
        }
        if (links.size() > 0) {
            return links;
        }
        return null;

    }

    /**
     * Метод по нормализации ссылки:
     * - добавляем https:// при необходимости
     * - убираем лишние символы
     * @param links - список ссылок для нормализации
     * @return список нормализованных ссылок
     */
    static List<String> normalize(List<String> links) {
        List<String> normalized_links = new TreeList<String>();

        for (String link : links) {
            char lastEl = link.charAt(link.length() - 1);
            if ((lastEl == ')') || (lastEl == '.') || (lastEl == ':'))
                link = link.substring(0, link.length() - 1);

            Pattern pattern = Pattern.compile("[0-9]{2,256}\\.[0-9]{2,256}");
            Matcher matcher = pattern.matcher(link);
            if (matcher.find())
                continue;

            pattern = Pattern.compile("http(s)?:\\/\\/");
            matcher = pattern.matcher(link);
            if (!matcher.find())
                link = "http://" + link;
            normalized_links.add(link);
        }
        return normalized_links;
    }


    /**
     * Метод по парсингу данных по каждому дню
     * @param query - запрос для поиска новостей
     * @param daysCount - количество дней, по которым нужно получить данные
     * @return - словарь, где ключ - дата, значение - все комментарии за текущую дату
     */
    Map<Integer, List<SearchResponse>> parseByDays(String query, int daysCount) throws ClientException, ApiException, ParseException, SQLException, NoSuchAlgorithmException, IOException {

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        Map<String, Map<Integer, List<SearchResponse>>> dataMap = new TreeMap<String, Map<Integer, List<SearchResponse>>>();
        Map<Integer, List<SearchResponse>> days = new TreeMap<>();

        int start = 10;
        int totalPublicationsCount = 0;

        List<SearchResponse> searchResponseList;
        DataBase db = new DataBase();
        Map<String, Integer> publicationsCountMap = new TreeMap<>();
        for (int i = start; i < start + 3; i++) {
            totalPublicationsCount = 0;
            String startDate = "2020-10-" + i;
            long startDay = dateFormat.parse(startDate + "T00:00:00.000-0000").getTime();
            long endDay = dateFormat.parse("2020-10-" + (i + 1) + "T00:00:00.000-0000").getTime();

            searchResponseList = parse(query, startDay, endDay);

            for (SearchResponse sr : searchResponseList) {
                totalPublicationsCount += sr.getItems().size();
            }
            publicationsCountMap.put(startDate, totalPublicationsCount);

            days.put(i, searchResponseList);
        }

        db.setNewsCount(publicationsCountMap, query);
        return days;
    }

    /**
     * Метод по получению данных по одной странице (200 новостей)
     * @param query - текущий запрос
     * @param startDate - начало периода
     * @param endDate - конец периода
     * @return - результат запроса
     */
    List<SearchResponse> parse(String query, long startDate, long endDate) throws ClientException, ApiException, SQLException, NoSuchAlgorithmException, IOException {

        String index = "";
        int count = 200;
        int limit = 0;

        List<SearchResponse> searchResponseList = new TreeList<SearchResponse>();
        String next_from = "";

        do {
            SearchResponse result;
            try {
                result = createQuery(query, startDate, endDate, next_from); //делаем запрос и получаем 1000 новостей
                next_from = getNext_from(query, startDate, endDate, next_from);

                if (result.getItems().size() > 0) {
                    searchResponseList.add(result);
                }
            } catch (Exception e) {
                // System.out.println(e.getMessage());
                next_from = null;
            }
        }
        while (next_from != null);

        db.sendData(query, searchResponseList);
        return searchResponseList;

    }


    /**
     * Вспомогательный метод по получению токена следующей страницы
     * @param query - текущий запрос
     * @param startDate - начало периода
     * @param endDate - конец периода
     * @param next_from - предыдущий токен
     * @return - текущий токен
     */
    String getNext_from(String query, long startDate, long endDate, String next_from) throws ClientException {


        NewsfeedSearchQuery newsfeedSearchQuery = this.vkClient.newsfeed().search(this.userActor)
                .count(200).startTime((int) (startDate / 1000)).endTime((int) (endDate / 1000)).startFrom(next_from).q(query);
        String result = newsfeedSearchQuery.executeAsRaw().getContent();


        int start_index = result.indexOf("next_from");
        if (start_index == -1) {
            return null;
        }
        String sub_str = result.substring(start_index);
        int second_index = sub_str.indexOf("count");

        String final_res = sub_str.substring(0, second_index - 3).substring(12);
        return final_res.replace("\\", "");
    }


    /**
     * Метод по созданию запроса на получение новостей
     * @param query - текст запроса
     * @param startDate - параметр даты начала
     * @param endDate - параметр даты конца
     * @param next_from - текущий токен
     */
    public SearchResponse createQuery(String query, long startDate, long endDate, String next_from) throws ClientException, ApiException, InterruptedException {
        NewsfeedSearchQuery newsfeedSearchQuery = this.vkClient.newsfeed().search(this.userActor)
                .count(200).startTime((int) (startDate / 1000)).endTime((int) (endDate / 1000)).startFrom(next_from).q(query);
        try {
            return newsfeedSearchQuery.execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return null;
    }

}