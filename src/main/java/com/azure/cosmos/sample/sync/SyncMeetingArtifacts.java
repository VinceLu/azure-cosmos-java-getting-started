// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.sync;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.MeetingMetadata;
import com.azure.cosmos.util.CosmosPagedIterable;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SyncMeetingArtifacts {
    // query parameters
    private static final int _maxBufferedItemCount = 100;
    private static final int _maxDegreeOfParallelism = 1000;
    private static final int _preferredPageSize = 10;

    private static Gson gson = new Gson();
    // For POJO/JsonNode interconversion
    private static final ObjectMapper OBJECT_MAPPER = Utils.getSimpleObjectMapper();


    private CosmosClient _cosmosClient;

    private final String _databaseName = "MeetingArtifacts";
    private final String _containerName = "MeetingMetadata";

    private CosmosDatabase _database;
    private CosmosContainer _container;
    private static int THROUGHPUT = 400;
    public static final DateTimeFormatter DTF = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    protected static Logger LOG = LoggerFactory.getLogger(SyncMain.class.getSimpleName());

    public void close() {
        _cosmosClient.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    //  <Main>
    public static void main(String[] args) {
        SyncMeetingArtifacts p = new SyncMeetingArtifacts();

        try {
            LOG.info("Starting SYNC MeetingArtifacts");
            p.getStarted();
            LOG.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            LOG.error("Cosmos getStarted failed with", e);
        } finally {
            LOG.info("Closing the client");
            p.close();
        }
        System.exit(0);
    }

    //  </Main>

    private void getStarted() throws Exception {
        LOG.info("Using Azure Cosmos DB endpoint: {}", AccountSettings.HOST);

        //  Create sync client
        //  <CreateSyncClient>
        _cosmosClient = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                //  Setting the preferred location to Cosmos DB Account region
                //  West US is just an example. User should set preferred location to the Cosmos DB region closest to the application
                .preferredRegions(Collections.singletonList("West US"))
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();

        //  </CreateSyncClient>

        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //  Setup family items to create
        List<MeetingMetadata>  meetingMetadataList = new ArrayList<>();
        Set<String> attendees = new HashSet();
        attendees.add("Foo");
        attendees.add("Bar");
        attendees.add("Var");
        LocalDateTime meetingStartTime = LocalDateTime.of(2020, 12,5,10,30);
        LocalDateTime meetingEndTime = LocalDateTime.of(2020, 12,5,11,30);
        meetingMetadataList.add(new MeetingMetadata("12345", attendees, meetingStartTime.toString(), meetingEndTime.toString()));

        List<String> keys = upsertDocumentsSync(meetingMetadataList);

        LOG.info("Reading items.");
        List<MeetingMetadata> result = getMeetingMetadata(meetingMetadataList.stream().map(MeetingMetadata::getId).collect(Collectors.toList()));

        LOG.info("Querying items.");
        String sql = "SELECT * FROM root r where r.entityType = 'MeetingMetadata'";
        List<MeetingMetadata> queryResult = queryItems(sql);
        if (!result.equals(queryResult)) {
            LOG.error("result not equal!");
        }
    }

    private void createDatabaseIfNotExists() throws Exception {
        LOG.info("Create database {} if not exists.", _databaseName);

        //  Create database if not exists
        //  <CreateDatabaseIfNotExists>
        CosmosDatabaseResponse cosmosDatabaseResponse = _cosmosClient.createDatabaseIfNotExists(_databaseName);
        _database = _cosmosClient.getDatabase(cosmosDatabaseResponse.getProperties().getId());
        //  </CreateDatabaseIfNotExists>

        LOG.info("Checking database {} completed!\n", _database.getId());
    }

    private void createContainerIfNotExists() throws Exception {
        LOG.info("Create container {} if not exists.", _containerName);

        //  Create container if not exists
        //  <CreateContainerIfNotExists>
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(_containerName, "/id");

        //  Create container with 400 RU/s
        CosmosContainerResponse cosmosContainerResponse =
                _database.createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(THROUGHPUT));
        _container = _database.getContainer(cosmosContainerResponse.getProperties().getId());
        //  </CreateContainerIfNotExists>

        LOG.info("Checking container {} completed!\n", _container.getId());
    }

    public List<String> upsertDocumentsSync(List<MeetingMetadata> meetingMetadataList) throws Exception {
        double totalRequestCharge = 0;
        List<String> keys = new ArrayList<>();
        for (MeetingMetadata meetingMetadata : meetingMetadataList) {

            //  <UpsertItem>
            //  Upsert item using container that we created using sync client

            //  Use meeting id as partitionKey for cosmos item
            //  Using appropriate partition key improves the performance of database operations
            //  serialize Object to json
            JsonNode meetingMetaJson = OBJECT_MAPPER.valueToTree(meetingMetadata);
            ((ObjectNode) meetingMetaJson).put("entityType", "MeetingMetadata");
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();

            CosmosItemResponse<JsonNode> item = _container.upsertItem(meetingMetaJson,
                    new PartitionKey(meetingMetadata.getId()), cosmosItemRequestOptions);
            //  </CreateItem>

            //  Get request charge and other properties like latency, and diagnostics strings, etc.
            LOG.info("Created item with request charge of {} within duration {}",
                    item.getRequestCharge(), item.getDuration());
            totalRequestCharge += item.getRequestCharge();
            keys.add(meetingMetadata.getId());
        }
        LOG.info("Created {} items with total request charge of {}",
                meetingMetadataList.size(),
                totalRequestCharge);
        return keys;
    }


    // read items from DB sync
    private List<JsonNode> readItemsSync(List<String> keys) {
        //  Using partition key for point read scenarios.
        //  This will help fast look up of items because of partition key
        List<JsonNode> res = new ArrayList<>();
        keys.forEach(key -> {
            try {
                CosmosItemResponse<JsonNode> item = _container.readItem(key, new PartitionKey(key), JsonNode.class);
                double requestCharge = item.getRequestCharge();
                Duration requestLatency = item.getDuration();
                LOG.info("Item successfully read with key {} with a charge of {} and within duration {}",
                        key, requestCharge, requestLatency);
                res.add(item.getItem());
            } catch (CosmosException e) {
                LOG.error("Read Item failed with", e);
            }
        });
        return res;
    }

    public List<MeetingMetadata> getMeetingMetadata(List<String> keys) {
        List<MeetingMetadata> result = new ArrayList<>();
        List<JsonNode> res = readItemsSync(keys);
        for (JsonNode jsonDoc : res) {
            if (jsonDoc != null) {
                // de-serialize the document
                try {
                    result.add(OBJECT_MAPPER.treeToValue(jsonDoc, MeetingMetadata.class));
                } catch (JsonProcessingException ex) {
                    LOG.error("Error deserializing fetched MeetingMeta item!", ex);
                }
            } else {
                LOG.error("fetched jsonDoc is null!");
            }
        }
        return result;
    }

    public List<MeetingMetadata> queryItems(String sql) {
        List<MeetingMetadata> results = new ArrayList<>();
        // Set some common query options
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        //queryOptions.setEnableCrossPartitionQuery(true); //No longer necessary in SDK v4
        //  Set query metrics enabled to get metrics around query executions
        queryOptions.setQueryMetricsEnabled(true);
        queryOptions.setMaxBufferedItemCount(_maxBufferedItemCount);
        queryOptions.setMaxDegreeOfParallelism(_maxDegreeOfParallelism);


        CosmosPagedIterable<JsonNode> pagedIterable = _container.queryItems(sql, queryOptions, JsonNode.class);

        pagedIterable.iterableByPage(_preferredPageSize).forEach(cosmosItemPropertiesFeedResponse -> {
            LOG.info("Got a page of query result with {} items(s) and request charge of {}",
                    cosmosItemPropertiesFeedResponse.getResults().size(), cosmosItemPropertiesFeedResponse.getRequestCharge());

            List<JsonNode> res = cosmosItemPropertiesFeedResponse.getResults();
            for (JsonNode item : res) {
                try {
                    results.add(OBJECT_MAPPER.treeToValue(item, MeetingMetadata.class));
                } catch (JsonProcessingException ex) {
                    LOG.error("Error deserializing JsonNode {} into MeetingMetadata object ", item.toString(), ex);
                }
            }
        });
        return results;
    }
}
