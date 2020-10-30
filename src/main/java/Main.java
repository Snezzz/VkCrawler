import au.com.bytecode.opencsv.CSVWriter;
import org.apache.commons.collections4.list.TreeList;
import org.apache.oltu.oauth2.common.exception.OAuthProblemException;
import org.apache.oltu.oauth2.common.exception.OAuthSystemException;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;


public class Main {
    public static void main(String [] argv) throws IOException, OAuthSystemException, OAuthProblemException, SQLException {

        System.out.println("Загрузить данные - 1, получить статистику - 2");
        Scanner scanner = new Scanner(System.in);
        String request = scanner.next();

        if(!request.equals("1")&&!request.equals("2"))
            System.exit(0);
        if(request.equals("1"))
            loadData();
        else if (request.equals("2"))
            loadStatistics();
        System.out.println("finished");
    }

    private static void loadData() throws OAuthSystemException, OAuthProblemException, IOException {

        Set<String> queries = new HashSet<String>();
        queries.add("спбгу");
        queries.add("spbu");
        queries.add("мгу");
        queries.add("mgu");
        queries.add("Lomonosov Moscow State University");

        List<Integer> userIds = new TreeList<>();
        userIds.add(1);
        userIds.add(2);
        userIds.add(3);
        userIds.add(4);
        userIds.add(5);

        new MultiThread(queries,userIds);
    }

    private static void loadStatistics() throws IOException, SQLException {

        DataBase db = new DataBase();

        //Общее количество комментариев для каждого университета за выделенный промежуток времени
        int totalSPBUCommentsCount = db.getTotalPublicationsCount("спбгу");
        int totalMGUCommentsCount = db.getTotalPublicationsCount("мгу");

        //Количество публикующих контент пользователей
        int userSPBUCount = db.getUsersCount("спбгу");
        int userMGUCount = db.getUsersCount("мгу");

        //данные о количестве публикаций в день
        ResultSet spbuData = db.getGraphicData("спбгу");
        ResultSet mguData = db.getGraphicData("мгу");

        int daysCount = db.getDaysCount();
        sendCountData(daysCount, totalSPBUCommentsCount, totalMGUCommentsCount, userSPBUCount, userMGUCount);
        sendData("спбгу",spbuData);
        sendData("мгу",mguData);
    }

    static void sendCountData(int daysCount,
                              int totalSPBUCommentsCount,
                              int totalMGUCommentsCount,
                              int userSPBUCount,
                              int userMGUCount) throws IOException {
        String csv =  "Total data.csv";
        CSVWriter writer = new CSVWriter(new FileWriter(csv));

        String [] data = new String[2];

        data[0] = "количество комментариев об СПбГУ за период в " + daysCount + " дней";
        data[1] = String.valueOf(totalSPBUCommentsCount);
        writer.writeNext(data);

        data[0] = "количество комментариев об МГУ за период в " + daysCount + " дней";
        data[1] = String.valueOf(totalMGUCommentsCount);
        writer.writeNext(data);

        data[0] = "количество авторов новостей об СПбГУ за период в " + daysCount + " дней";
        data[1] = String.valueOf(userSPBUCount);
        writer.writeNext(data);

        data[0] = "количество авторов новостей об МГУ за период в " + daysCount + " дней";
        data[1] = String.valueOf(userMGUCount);
        writer.writeNext(data);
        //close the writer
        writer.close();

    }

    static void sendData(String university, ResultSet data) throws IOException, SQLException {
        String csv = university + "Data.csv";
        CSVWriter writer = new CSVWriter(new FileWriter(csv));

        String [] metaValues = new String[2];
        metaValues[0] = "day";
        metaValues[1] = "news count";
        writer.writeNext(metaValues);
        while(data.next()){
            String [] values = new String[2];
            values[0] = data.getString(1);
            values[1] = data.getString(2);
            writer.writeNext(values);
        }
        //close the writer
        writer.close();
    }
}
