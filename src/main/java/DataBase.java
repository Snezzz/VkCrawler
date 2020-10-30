import com.vk.api.sdk.objects.newsfeed.responses.SearchResponse;
import com.vk.api.sdk.objects.wall.WallpostFull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class DataBase {
    private Connection c;

    DataBase() throws IOException {

        Properties properties = new Properties();
        properties.load(new FileInputStream(new File("src/main/resources/db.properties")));
        String url = properties.getProperty("db.url");
        String user = properties.getProperty("db.user");
        String password = properties.getProperty("db.password");

        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection(url, user, password);
            c.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод по отправлению данных в таблицы 'news' и 'links'
     * @param type - мгу или спбгу
     * @param responses - список комментариев
     */
    public void sendData(String type, List<SearchResponse> responses) throws SQLException, NoSuchAlgorithmException {

        Map<String,List<String>> result = new TreeMap<String, List<String>>();
        //responses
        PreparedStatement stmt;
        String sql = "INSERT INTO vk_crawler.news VALUES (?,?,?,?,?,?,?,?,?)";
        stmt = c.prepareStatement(sql); //открываем соединение
        int j = 0;
        for(SearchResponse response: responses) {
            int i = 0;

            for (WallpostFull wallpost : response.getItems()) {
                String news_id =  UUID.randomUUID().toString();
                String author_id = String.valueOf(wallpost.getFromId().toString());
                String author = "id" + author_id.substring(1,author_id.length());
                stmt.setString(1,  news_id);
                stmt.setString(2, author);
                stmt.setInt(3, wallpost.getLikes().getCount());
                try {
                    stmt.setInt(4, wallpost.getViews().getCount());
                }
                catch (Exception e){
                    stmt.setInt(4, 0);
                }

                stmt.setInt(5, wallpost.getComments().getCount());
                stmt.setInt(6, wallpost.getReposts().getCount());
                stmt.setString(7, getDate(wallpost.getDate()));
                stmt.setString(8, wallpost.getText());
                stmt.setString(9, type);
                stmt.addBatch();

                //получаем все ссылки
                List<String> links = NewsParser.getLinks(wallpost.getText());

                if(links!= null) {
                    List<String> normalized_links = NewsParser.normalize(links);
                    result.put(news_id,normalized_links);
                }

               i++;
               j++;
            }

        }

        stmt.executeBatch();
        stmt.close();
        c.commit();

        //отправляем ссылки в БД
        sql = "INSERT INTO vk_crawler.links VALUES (?,?,?)";
        stmt = c.prepareStatement(sql); //открываем соединение
        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
            for (String link:entry.getValue()) {
                String id = UUID.randomUUID().toString();
                stmt.setString(1, id);
                stmt.setString(2,link);
                stmt.setString(3,entry.getKey());
                stmt.addBatch();
            }
        }
        stmt.executeBatch();
        stmt.close();
        c.commit();

    }

    String getDate(Integer vkDate){
        Date date = new java.util.Date(vkDate*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT-4"));
        return sdf.format(date);
    }

    /**
     * Метод по получению количества уникальных пользователей по новостям по текущему университету
     * @param university - текущий университет
     * @return - количество пользователей
     */
    public int getUsersCount(String university) throws SQLException {
        Statement st = c.createStatement();
        String query = "select count(DISTINCT news.author) FROM vk_crawler.news as news WHERE type = '" + university +"'";
        ResultSet resultSet = st.executeQuery(query);
        if (resultSet.next())
            return Integer.valueOf(resultSet.getString("count"));
        return 0;
    }

    /**
     * Метод по добавлению данных в таблицу 'news_count' для статистических расчетов
     * @param data - словарь, где ключ - дата, значение - общее количество новостей
     * @param query - текущий запрос
     */
    public void setNewsCount(Map<String,Integer> data, String query) throws SQLException {
        PreparedStatement stmt;
        String sql = "INSERT INTO vk_crawler.news_count VALUES (?,?,?,?)";
        stmt = c.prepareStatement(sql); //открываем соединение
        for (Map.Entry<String,Integer> entry: data.entrySet()){
            stmt.setString(1,UUID.randomUUID().toString());
            stmt.setString(2, entry.getKey());
            stmt.setInt(3,entry.getValue());
            if(query.equals("спбгу") || query.equals("spbu"))
                stmt.setString(4,"спбгу");
            if(query.equals("мгу") || query.equals("mgu") || query.equals("Lomonosov Moscow State University"))
                stmt.setString(4,"мгу");
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.close();
        c.commit();
    }

    /**
     * Метод по получению статистических данных
     * @return количество дней, по которым есть данные
     */
    public int getDaysCount() throws SQLException {
        Statement st = c.createStatement();
        String query = "select count(DISTINCT(day)) FROM vk_crawler.news_count";
        ResultSet resultSet = st.executeQuery(query);
        if (resultSet.next())
            return Integer.valueOf(resultSet.getString("count"));
        return 0;
    }


    /**
     * Метод по получению статистических данных для построения графика в дальнейшем
     * @param university - текущий университет
     * @return - список дней и количества комментариев по каждому дню
     */
    public ResultSet getGraphicData(String university) throws SQLException {
        Statement st = c.createStatement();

        String query = "select day,sum(count) FROM vk_crawler.news_count WHERE university='" + university + "' " +
                "group by day" ;
        return st.executeQuery(query);

    }

    /**
     * Метод по подсчету общего количества новостей для текущего университета на выбранное время
     * @param university - текущий университет
     * @return  - количество новостей
     */
    public int getTotalPublicationsCount(String university) throws SQLException {

        Statement st = c.createStatement();
        String query = "with TT as(select university, sum(count) as sum FROM vk_crawler.news_count " +
                "WHERE university = '"+university+"' group by day,university) " +
                "SELECT university, sum(sum) from TT GROUP BY university";

        ResultSet resultSet = st.executeQuery(query);
        if(resultSet.next())
            return Integer.valueOf(resultSet.getString("sum"));

        return 0;

    }
}
