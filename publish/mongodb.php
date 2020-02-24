<?php
declare(strict_types=1);
return [
    'default' => [
        'uri_options' => [
            'ssl' => true,
            'username' => env('MONGODB_USERNAME', ''),
            'password' => env('MONGODB_PASSWORD', ''),
//            'authMechanism' => env('MONGODB_AUTH_MECHANISM', 'SCRAM-SHA-256'),
            //设置复制集,没有不设置
//        'replicaSet' => 'rs0',
        ],
        'host' => env('MONGODB_HOST', '127.0.0.1'),
        'port' => env('MONGODB_PORT', 27017),
        'db' => env('MONGODB_DB', 'test'),
        'driver_options' => [],
        'migration' => [
            'path' => BASE_PATH . '/migrations/mongodb', // 迁移文件的路径
        ],
        'dsn' => '', // 支持直接使用url的方式连接mongodb
        'pool' => [
            'min_connections' => 1,
            'max_connections' => 100,
            'connect_timeout' => 10.0,
            'wait_timeout' => 3.0,
            'heartbeat' => -1,
            'max_idle_time' => (float)env('MONGODB_MAX_IDLE_TIME', 60),
        ],
    ],
];
