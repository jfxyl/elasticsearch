<?php

return [
    /*
     * Elasticsearch Connection Hosts
     */
    'hosts' => [
        'http://127.0.0.1:9200'
    ],

    /*
     * Elasticsearch Connection Retry Times
     */
    'connection_retry_times' => null,

    /*
     * --------------------------------------------------------------------------
     * Elasticsearch Connection Pool
     * --------------------------------------------------------------------------
     * \Elasticsearch\ConnectionPool\StaticNoPingConnectionPool::class
     * \Elasticsearch\ConnectionPool\StaticConnectionPool::class
     * \Elasticsearch\ConnectionPool\SimpleConnectionPool::class
     * \Elasticsearch\ConnectionPool\SniffingConnectionPool::class
     * --------------------------------------------------------------------------
     */
    'connection_pool' => \Elasticsearch\ConnectionPool\StaticNoPingConnectionPool::class,

    /*
     * --------------------------------------------------------------------------
     * Elasticsearch Connection Pool Selector
     * --------------------------------------------------------------------------
     * \Elasticsearch\ConnectionPool\Selectors\RoundRobinSelector::class
     * \Elasticsearch\ConnectionPool\Selectors\StickyRoundRobinSelector::class
     * \Elasticsearch\ConnectionPool\Selectors\RandomSelector::class
     * --------------------------------------------------------------------------
     */
    'selector' => \Elasticsearch\ConnectionPool\Selectors\RoundRobinSelector::class,

    /*
     * --------------------------------------------------------------------------
     * Elasticsearch Serializer
     * --------------------------------------------------------------------------
     * \Elasticsearch\Serializers\SmartSerializer::class
     * \Elasticsearch\Serializers\ArrayToJSONSerializer::class
     * \Elasticsearch\Serializers\EverythingToJSONSerializer::class
     * --------------------------------------------------------------------------
     */
    'serializer' => \Elasticsearch\Serializers\SmartSerializer::class,
];
