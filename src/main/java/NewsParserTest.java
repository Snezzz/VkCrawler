import com.vk.api.sdk.exceptions.ApiException;
import com.vk.api.sdk.exceptions.ClientException;
import com.vk.api.sdk.objects.newsfeed.responses.SearchResponse;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class NewsParserTest {
    private static NewsParser newsParser;

    @BeforeClass
    public static void setUp() throws Exception {
        newsParser = new NewsParser("user1");
    }

    @Test
    public void test_parse_spbu() throws NoSuchAlgorithmException, SQLException, ApiException, IOException, ClientException {
        List<SearchResponse> searchResponseList = newsParser.parse("спбгу", 1602374400000L, 1602460800000L);
        assertNotNull(searchResponseList);
    }

    @Test
    public void test_parse_spbu2() throws NoSuchAlgorithmException, SQLException, ApiException, IOException, ClientException {
        List<SearchResponse> searchResponseList = newsParser.parse("spbu", 1602374400000L, 1602460800000L);
        assertNotNull(searchResponseList);
    }

    @Test
    public void test_parse_msu() throws NoSuchAlgorithmException, SQLException, ApiException, IOException, ClientException {
        List<SearchResponse> searchResponseList = newsParser.parse("Lomonosov Moscow State University", 1602374400000L, 1602460800000L);
        assertNotNull(searchResponseList);
    }

    @Test
    public void test_parse_msu2() throws NoSuchAlgorithmException, SQLException, ApiException, IOException, ClientException {
        List<SearchResponse> searchResponseList = newsParser.parse("мгу", 1602374400000L, 1602460800000L);
        assertNotNull(searchResponseList);
    }

    @Test
    public void test_parse_msu3() throws NoSuchAlgorithmException, SQLException, ApiException, IOException, ClientException {
        List<SearchResponse> searchResponseList = newsParser.parse("mgu", 1602374400000L, 1602460800000L);
        assertNotNull(searchResponseList);
    }

    @Test
    public void test_text_without_link() {
        String text = "text without link";
        List<String> links = NewsParser.getLinks(text);
        assertNull(links);
    }

    @Test
    public void test_link_with_http() {
        String text = "text http://foo.com/blah_blah text";
        List<String> links = NewsParser.getLinks(text);
        String link = links.get(0);
        assertEquals(link, "http://foo.com/blah_blah");
    }

    @Test
    public void test_link_with_https() {
        String text = "text https://foo.com/blah_blah text";
        List<String> links = NewsParser.getLinks(text);
        String link = links.get(0);
        assertEquals(link, "https://foo.com/blah_blah");
    }

    @Test
    public void test_link_without_protocol() {
        String text = "text www.foo.com/blah_blah text";
        List<String> links = NewsParser.getLinks(text);
        String link = links.get(0);
        assertEquals(link, "www.foo.com/blah_blah");
    }

    @Test
    public void test_several_links() {
        String text = "text www.foo.com/blah_blah text https://foo.com/blah_blah text";
        List<String> links = NewsParser.getLinks(text);
        assertEquals(links.size(), 2);
    }

    @Test
    public void test_normalization(){
        String link = "www.foo.com/blah_blah";
        List<String> links = Collections.singletonList(link);
        String normalizedLink = NewsParser.normalize(links).get(0);
        assertEquals(normalizedLink, "http://www.foo.com/blah_blah");
    }

    @Test
    public void test_wrong_date_for_create_query() throws InterruptedException, ClientException, ApiException {
        SearchResponse seaerchResponse = newsParser.createQuery("spbu", 10, -2, "");
        assertNull(seaerchResponse);
    }

    @Test
    public void test_valid_date_for_create_query() throws InterruptedException, ClientException, ApiException {
        SearchResponse seaerchResponse = newsParser.createQuery("spbu", 1602277200, 1602363600, "");
        assertEquals(SearchResponse.class, seaerchResponse.getClass());
    }

    @Test
    public void test_wrong_date_for_getNext_from() throws InterruptedException, ClientException, ApiException {
        String next_from = newsParser.getNext_from("spbu", 10, -5, "");
        assertNull(next_from);
    }

}