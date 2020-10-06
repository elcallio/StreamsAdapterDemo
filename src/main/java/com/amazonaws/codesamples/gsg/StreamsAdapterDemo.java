/*
* Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*  http://aws.amazon.com/apache2.0
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package com.amazonaws.codesamples.gsg;

import static com.amazonaws.codesamples.gsg.StreamsAdapterDemoHelper.createTable;
import static com.amazonaws.codesamples.gsg.StreamsAdapterDemoHelper.describeTable;
import static com.amazonaws.codesamples.gsg.StreamsAdapterDemoHelper.putItem;
import static com.amazonaws.codesamples.gsg.StreamsAdapterDemoHelper.scanTable;
import static com.amazonaws.codesamples.gsg.StreamsAdapterDemoHelper.updateItem;
import static com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream.TRIM_HORIZON;

import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class StreamsAdapterDemo {
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		ArgumentParser parser = ArgumentParsers.newFor("StreamAdapterDemo").build().defaultHelp(true)
				.description("Replicate a simple table using DynamoDB/Alternator Streams.");

		parser.addArgument("--aws").setDefault(false).help("Run against AWS");
		parser.addArgument("-c", "--cloudwatch").setDefault(false).help("Enable Cloudwatch");
		parser.addArgument("-e", "--endpoint").setDefault(new URL("http://localhost:8000"))
				.help("DynamoDB/Alternator endpoint");
		parser.addArgument("-se", "--streams-endpoint").setDefault(new URL("http://localhost:8000"))
				.help("DynamoDB/Alternator streams endpoint");

		parser.addArgument("-u", "--user").help("Credentials username");
		parser.addArgument("-p", "--password").help("Credentials password");
		parser.addArgument("-r", "--region").setDefault("us-east-1").help("AWS region");
		parser.addArgument("-t", "--table-prefix").setDefault("KCL-Demo").help("Demo table name prefix");

		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}

		String tablePrefix = ns.getString("table_prefix");

		AmazonDynamoDBClientBuilder b = AmazonDynamoDBClientBuilder.standard();
		AmazonDynamoDBStreamsClientBuilder sb = AmazonDynamoDBStreamsClientBuilder.standard();
		AmazonCloudWatch cloudWatchClient = null;

		if (!ns.getBoolean("aws")) {
			if (ns.getString("endpoint") != null) {
				b.withEndpointConfiguration(
						new EndpointConfiguration(ns.getString("endpoint"), ns.getString("region")));
				sb.withEndpointConfiguration(b.getEndpoint());
			}
			if (ns.getString("streams_endpoint") != null) {
				sb.withEndpointConfiguration(
						new EndpointConfiguration(ns.getString("streams_endpoint"), ns.getString("region")));
			}
			if (ns.getString("user") != null) {
				b.withCredentials(new AWSStaticCredentialsProvider(
						new BasicAWSCredentials(ns.getString("user"), ns.getString("password"))));
				sb.withCredentials(b.getCredentials());
			}
		}

		System.out.println("Starting demo...");

		String srcTable = tablePrefix + "-src";
		String destTable = tablePrefix + "-dest";

		IRecordProcessorFactory recordProcessorFactory = new StreamsRecordProcessorFactory(b, destTable);

		AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(sb.build());
		AmazonDynamoDB dynamoDBClient = b.build();

		if (ns.getBoolean("cloudwatch")) {
			cloudWatchClient = AmazonCloudWatchClientBuilder.standard().withCredentials(b.getCredentials())
					.withClientConfiguration(b.getClientConfiguration()).build();
		}

		ExecutorService xs = Executors.newWorkStealingPool(10);

		try {
			String streamArn = setUpTables(dynamoDBClient, tablePrefix);

			KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("streams-adapter-demo",
					streamArn, b.getCredentials(), "streams-demo-worker").withMaxRecords(1)
							.withInitialPositionInStream(TRIM_HORIZON);

			System.out.println("Creating worker for stream: " + streamArn);
			Worker worker = new Worker(recordProcessorFactory, workerConfig, adapterClient, dynamoDBClient,
					cloudWatchClient, xs);
			System.out.println("Starting worker...");
			Thread t = new Thread(worker);
			t.start();

			ScanResult sr = scanTable(dynamoDBClient, srcTable);
			ScanResult dr = null;

			for (int i = 0; i < 250; ++i) {
				Thread.sleep(1000);
				dr = scanTable(dynamoDBClient, destTable);
				if (!dr.getCount().equals(sr.getCount())) {
					continue;
				}
				if (sr.getItems().equals(dr.getItems())) {
					break;
				}
			}

			worker.shutdown();
			t.join();

			if (dr != null && sr.getItems().equals(dr.getItems())) {
				System.out.println("Scan result is equal.");
			} else {
				System.out.println("Tables are different!");
			}

			System.out.println("Done.");
		} finally {
			cleanup(dynamoDBClient, tablePrefix);
		}
	}

	private static String setUpTables(AmazonDynamoDB dynamoDBClient, String tablePrefix) throws TimeoutException {
		String srcTable = tablePrefix + "-src";
		String destTable = tablePrefix + "-dest";
		String streamArn = createTable(dynamoDBClient, srcTable);
		createTable(dynamoDBClient, destTable);

		awaitTableCreation(dynamoDBClient, srcTable);
		performOps(dynamoDBClient, srcTable);

		return streamArn;
	}

	private static void awaitTableCreation(AmazonDynamoDB dynamoDBClient, String tableName) throws TimeoutException {
		Integer retries = 0;
		Boolean created = false;
		while (!created && retries < 100) {
			DescribeTableResult result = describeTable(dynamoDBClient, tableName);
			created = result.getTable().getTableStatus().equals("ACTIVE");
			if (created) {
				System.out.println("Table is active.");
				return;
			} else {
				retries++;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// do nothing
				}
			}
		}
		throw new TimeoutException("Timeout after table creation.");
	}

	private static void performOps(AmazonDynamoDB dynamoDBClient, String tableName) {
		putItem(dynamoDBClient, tableName, "101", "test1");
		updateItem(dynamoDBClient, tableName, "101", "test2");
		// deleteItem(dynamoDBClient, tableName, "101");
		putItem(dynamoDBClient, tableName, "102", "demo3");
		updateItem(dynamoDBClient, tableName, "102", "demo4");
		// deleteItem(dynamoDBClient, tableName, "102");
	}

	private static void cleanup(AmazonDynamoDB dynamoDBClient, String tablePrefix) {
		String srcTable = tablePrefix + "-src";
		String destTable = tablePrefix + "-dest";
		dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(srcTable));
		dynamoDBClient.deleteTable(new DeleteTableRequest().withTableName(destTable));
	}

}
