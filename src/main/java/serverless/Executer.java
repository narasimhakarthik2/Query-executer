package serverless;

import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import serverless.configuration.AthenaConfiguration;
import serverless.constants.AthenaConstants;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Handler for requests to Lambda function.
 */
public class Executer implements RequestHandler<S3Event, String> {

    private static final Map<String, String> queries = new HashMap<>();

    static {
        queries.put("Complete Table", "SELECT * FROM invoice_details");
        queries.put("No of organizationId's", "SELECT COUNT(Distinct(organizationId)) AS no_of_organizationIds FROM invoice_details");
        queries.put("No of Distinct country's", "SELECT COUNT(Distinct(country)) AS no_of_countries FROM invoice_details");
    }

    @Override
    public String handleRequest(S3Event input, Context context) {
        AthenaConfiguration config = new AthenaConfiguration();
        AmazonAthena athenaClient = config.AthenaClient();

        LocalDateTime dateTime = LocalDateTime.now();
        for (Map.Entry<String, String> query : queries.entrySet()) {
            String folderPath = dateTime.toString() + "/" + query.getKey() + "/";
            String queryExecutionId = AthenaConfiguration.submitAthenaQuery(
                    athenaClient,
                    AthenaConstants.ATHENA_DATABASE,
                    AthenaConstants.ATHENA_OUTPUT_BUCKET + folderPath,
                    query.getValue());

            try {
                AthenaConfiguration.waitForQueryToComplete(athenaClient, queryExecutionId);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            AthenaConfiguration.processResultRows(athenaClient, queryExecutionId);
        }
        return "200";
    }
}
