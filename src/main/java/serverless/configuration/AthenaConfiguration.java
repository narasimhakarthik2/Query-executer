package serverless.configuration;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.*;
import serverless.constants.AthenaConstants;

import java.util.List;


public class AthenaConfiguration {


    public final AmazonAthena AthenaClient() {
        AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(AthenaConstants.ACCESS_KEY, AthenaConstants.SECRET_KEY));

        AmazonAthenaClientBuilder builder = AmazonAthenaClientBuilder.standard()
                .withRegion(Regions.US_WEST_2)
                .withCredentials(provider);
        return builder.build();
    }

    public static String submitAthenaQuery(AmazonAthena athenaClient, String athenaDatabase, String athenaOutputBucket, String athenaQuery) {
        // The QueryExecutionContext allows us to set the Database.
        QueryExecutionContext queryExecutionContext = new QueryExecutionContext().withDatabase(athenaDatabase);

        // The result configuration specifies where the results of the query should go in S3 and encryption options
        ResultConfiguration resultConfiguration = new ResultConfiguration()
                // You can provide encryption options for the output that is written.
                // .withEncryptionConfiguration(encryptionConfiguration)
                .withOutputLocation(athenaOutputBucket);

        // Create the StartQueryExecutionRequest to send to Athena which will start the query.
        StartQueryExecutionRequest startQueryExecutionRequest = new StartQueryExecutionRequest()
                .withQueryString(athenaQuery)
                .withQueryExecutionContext(queryExecutionContext)
                .withResultConfiguration(resultConfiguration);

        StartQueryExecutionResult startQueryExecutionResult = athenaClient.startQueryExecution(startQueryExecutionRequest);
        return startQueryExecutionResult.getQueryExecutionId();
    }


    /**
     * Wait for an Athena query to complete, fail or to be cancelled. This is done by polling Athena over an
     * interval of time. If a query fails or is cancelled, then it will throw an exception.
     */

    public static void waitForQueryToComplete(AmazonAthena athenaClient, String queryExecutionId) throws InterruptedException {
        GetQueryExecutionRequest getQueryExecutionRequest = new GetQueryExecutionRequest()
                .withQueryExecutionId(queryExecutionId);

        GetQueryExecutionResult getQueryExecutionResult = null;
        boolean isQueryStillRunning = true;
        while (isQueryStillRunning) {
            getQueryExecutionResult = athenaClient.getQueryExecution(getQueryExecutionRequest);
            String queryState = getQueryExecutionResult.getQueryExecution().getStatus().getState();
            if (queryState.equals(QueryExecutionState.FAILED.toString())) {
                throw new RuntimeException("Query Failed to run with Error Message: " + getQueryExecutionResult.getQueryExecution().getStatus().getStateChangeReason());
            } else if (queryState.equals(QueryExecutionState.CANCELLED.toString())) {
                throw new RuntimeException("Query was cancelled.");
            } else if (queryState.equals(QueryExecutionState.SUCCEEDED.toString())) {
                isQueryStillRunning = false;
            } else {
                // Sleep an amount of time before retrying again.
                Thread.sleep(AthenaConstants.SLEEP_AMOUNT_IN_MS);
            }
            System.out.println("Current Status is: " + queryState);
        }
    }

    /**
     * This code calls Athena and retrieves the results of a query.
     * The query must be in a completed state before the results can be retrieved and
     * paginated. The first row of results are the column headers.
     */
    public static void processResultRows(AmazonAthena athenaClient, String queryExecutionId) {
        GetQueryResultsRequest getQueryResultsRequest = new GetQueryResultsRequest()
                // Max Results can be set but if its not set,
                // it will choose the maximum page size
                // As of the writing of this code, the maximum value is 1000
                // .withMaxResults(1000)
                .withQueryExecutionId(queryExecutionId);

        GetQueryResultsResult getQueryResultsResult = athenaClient.getQueryResults(getQueryResultsRequest);
        while (true) {
            // If nextToken is null, there are no more pages to read. Break out of the loop.
            if (getQueryResultsResult.getNextToken() == null) {
                break;
            }
            getQueryResultsResult = athenaClient.getQueryResults(
                    getQueryResultsRequest.withNextToken(getQueryResultsResult.getNextToken()));
        }
    }
}