package stormapplied.githubcommits.datasource;


import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.utils.Utils;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author idumchykov
 * @since 5/3/18
 */
@Component
public class MockedQueue implements Serializable {

    private Queue<String> queue;

    public String getMessage() {
        Utils.sleep(100);
        return queue.poll();
    }

    @PostConstruct
    private void init() {
        try {
            List<String> commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("changelog.txt"),
                    Charset.defaultCharset().name());
            queue = new LinkedList<>(commits);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
