<?php
declare(strict_types=1);
namespace Phper666\MongoDb;

use Hyperf\Contract\ConnectionInterface;
use MongoDB\BSON\JavascriptInterface;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Operation\Explainable;
use Phper666\MongoDb\Exception\MongoDBException;
use Hyperf\Pool\Connection;
use Hyperf\Pool\Exception\ConnectionException;
use Hyperf\Pool\Pool;
use MongoDB\BSON\ObjectId;
use MongoDB\Driver\Command;
use MongoDB\Driver\Exception\AuthenticationException;
use MongoDB\Driver\Exception\Exception;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Driver\Manager;
use Phper666\MongoDb\Utils\Arr;
use Psr\Container\ContainerInterface;

class MongoDbConnection extends Connection implements ConnectionInterface
{
    /**
     * @var Manager
     */
    protected $connection;

    /**
     * @var Client
     */
    private $mongoClient;

    /**
     * @var array
     */
    protected $config;

    /**
     * MongoDbConnection constructor.
     * @param ContainerInterface $container
     * @param Pool               $pool
     * @param array              $config
     * @throws MongoDBException
     */
    public function __construct(ContainerInterface $container, Pool $pool, array $config)
    {
        parent::__construct($container, $pool);
        $this->config = $config;
        $this->reconnect();
    }

    /**
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param string $namespace
     * @param array  $collectionOptions
     * @return Collection
     */
    protected function collection(string $namespace, array $collectionOptions = [])
    {
        $collection = make(Collection::class, [$this->mongoClient->getManager(), $this->config['db'], $namespace, $collectionOptions]);
        return $collection;
    }

    /**
     * @param array $options
     * @return Database
     */
    protected function database(array $options = [])
    {
        $database = make(Database::class, [$this->mongoClient->getManager(), $this->config['db'], $options]);
        return $database;
    }

    /**
     * @return $this
     * @throws ConnectionException
     * @throws Exception
     * @throws MongoDBException
     */
    public function getActiveConnection()
    {
        // TODO: Implement getActiveConnection() method.
        if ($this->check()) {
            return $this;
        }
        if (!$this->reconnect()) {
            throw new ConnectionException('Connection reconnect failed.');
        }
        return $this;
    }

    /**
     * Reconnect the connection.
     * @return bool
     * @throws MongoDBException
     */
    public function reconnect(): bool
    {
        // TODO: Implement reconnect() method.
        try {
            /**
             * http://php.net/manual/zh/mongodb-driver-manager.construct.php
             */
            $username = $this->config['username'] ?? '';
            $password = $this->config['password'] ?? '';
            $authMechanism = $this->config['authMechanism'] ?? '';
            $replicaSet = $this->config['replicaSet'] ?? '';
            $driverOptions = $this->config['driver_options'] ?? [];
            $uriOptions = $this->config['uri_options'] ?? [];
            if (empty($uriOptions['username'])) {
                $uriOptions['username'] = $username;
            }
            if (empty($uriOptions['password'])) {
                $uriOptions['password'] = $password;
            }
            if (empty($uriOptions['replicaSet']) && !empty($replicaSet)) {
                $uriOptions['replicaSet'] = $replicaSet;
            }
            if (empty($uriOptions['authMechanism']) && !empty($authMechanism)) {
                $uriOptions['authMechanism'] = $authMechanism;
            }
            $uri = sprintf(
                'mongodb://%s:%d/%s',
                $this->config['host'],
                $this->config['port'],
                $this->config['db']
            );
            if (!empty($this->config['url'])) {
                $uri = $this->config['url'];
                $uriOptions = [];
            }
            $this->mongoClient = new Client($uri, $uriOptions, $driverOptions);
        } catch (InvalidArgumentException $e) {
            throw MongoDBException::managerError('mongodb 连接参数错误:' . $e->getMessage());
        } catch (RuntimeException $e) {
            throw MongoDBException::managerError('mongodb uri格式错误:' . $e->getMessage());
        }
        $this->lastUseTime = microtime(true);
        return true;
    }

    /**
     * @return Client
     */
    public function getMongoClient()
    {
        return $this->mongoClient;
    }

    /**
     * Close the mongoClient.
     */
    public function close(): bool
    {
        // TODO: Implement close() method.
        return true;
    }

    /**
     * 返回满足条件的第一个数据
     *
     * @param string $namespace
     * @param array|object $filter  Query by which to filter documents
     * @param array  $options
     * @param array  $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function findOne(string $namespace, $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $options = ['limit' => 1] + $options;
            if (empty($options['typeMap'])) {
                $options['typeMap'] = ['root' => 'array', 'document' => 'array', 'array' => 'array'];
            }
            $cursor = $this->collection($namespace, $collectionOptions)->find($filter, $options);
            $result = [];

            foreach ($cursor as $document) {
                if (!empty($document['_id'])) {
                    $document['_id'] = (string)$document['_id'];
                }
                $document = (array)$document;
                $result = $document;
            }
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 返回满足filer的全部数据
     *
     * @param string $namespace
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function findAll(string $namespace, array $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $result = [];
            if (empty($options['typeMap'])) {
                $options['typeMap'] = ['root' => 'array', 'document' => 'array', 'array' => 'array'];
            }
            $cursor = $this->collection($namespace, $collectionOptions)->find($filter, $options);
            foreach ($cursor as $document) {
                if (!empty($document['_id'])) {
                    $document['_id'] = (string)$document['_id'];
                }
                $document = (array)$document;
                $result[] = $document;
            }
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 返回分页数据，默认每页10条
     *
     * @param string $namespace
     * @param int $limit
     * @param int $currentPage
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function findPagination(string $namespace, int $currentPage = 0, int $limit = 10, array $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            // 查询数据
            $data = [];
            $result = [];

            //每次最多返回10条记录
            if (!isset($options['limit']) || (int)$options['limit'] <= 0) {
                $options['limit'] = $limit;
            }

            if (!isset($options['skip']) || (int)$options['skip'] <= 0) {
                if ($currentPage <= 1) {
                    $options['skip'] = 0;
                } else {
                    $options['skip'] = ($currentPage - 1) * $limit;
                }
            }
            if (empty($options['typeMap'])) {
                $options['typeMap'] = ['root' => 'array', 'document' => 'array', 'array' => 'array'];
            }
            $cursor = $this->collection($namespace, $collectionOptions)->find($filter, $options);
            foreach ($cursor as $document) {
                if (!empty($document['_id'])) {
                    $document['_id'] = (string)$document['_id'];
                }
                $document = (array)$document;
                $data[] = $document;
            }

            $result['total'] = $this->countDocuments($namespace, $filter);
            $result['page_no'] = $currentPage;
            $result['page_size'] = $limit;
            $result['rows'] = $data;

        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }


    /**
     * 查找单个文档并删除它，返回原始文档
     *
     * @param string $namespace
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|null|bool
     * @throws MongoDBException
     */
    public function findOneAndDelete(string $namespace, $filter, array $options = [], array $collectionOptions = []): array
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            if (empty($options['typeMap'])) {
                $options['typeMap'] = ['root' => 'array', 'document' => 'array', 'array' => 'array'];
            }
            $result = $this->collection($namespace, $collectionOptions)->findOneAndDelete($filter, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 查找单个文档并替换它，返回原始文档或替换文件
     *
     * @param string $namespace
     * @param        $filter
     * @param        $replacement
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|null|bool
     * @throws MongoDBException
     */
    public function findOneAndReplace(string $namespace, $filter, $replacement, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            if (empty($options['typeMap'])) {
                $options['typeMap'] = ['root' => 'array', 'document' => 'array', 'array' => 'array'];
            }
            $result = $this->collection($namespace, $collectionOptions)->findOneAndReplace($filter, $replacement, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 查找单个文档并更新它，返回原始文档或更新后的文件
     *
     * @param string $namespace
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|null|bool
     * @throws MongoDBException
     */
    public function findOneAndUpdate(string $namespace, $filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            if (empty($options['typeMap'])) {
                $options['typeMap'] = ['root' => 'array', 'document' => 'array', 'array' => 'array'];
            }
            $result = $this->collection($namespace, $collectionOptions)->findOneAndUpdate($filter, $update, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 插入多个数据
     *
     * @param string $namespace
     * @param array  $documents
     * @param array  $options
     * @param array  $collectionOptions
     * @return mixed[]
     * @throws MongoDBException
     */
    public function insertMany(string $namespace, array $documents = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->insertMany($documents, $options)->getInsertedIds();
            foreach ($result as $k => $v) {
                $result[$k] = (string)$v;
            }
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 插入一个数据
     *
     * @param string $namespace
     * @param array  $documents
     * @param array  $options
     * @param array  $collectionOptions
     * @return mixed
     * @throws MongoDBException
     */
    public function insertOne(string $namespace, $document = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = (string)$this->collection($namespace, $collectionOptions)->insertOne($document, $options)->getInsertedId();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 更新匹配的到的所有数据
     *
     * @param string $namespace
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null
     * @throws MongoDBException
     */
    public function updateMany(string $namespace, $filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $result = $this->collection($namespace, $collectionOptions)->updateMany($filter, $update, $options)->getModifiedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 更新匹配的到的一条数据
     *
     * @param string $namespace
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null
     * @throws MongoDBException
     */
    public function updateOne(string $namespace, $filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $result = $this->collection($namespace, $collectionOptions)->updateOne($filter, $update, $options)->getModifiedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 数据更新,效果是满足filter的行,只更新$newObj中的$set出现的字段
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.update.php
     * $bulk->update(
     *   ['x' => 2],
     *   ['$set' => ['y' => 3]],
     *   ['multi' => false, 'upsert' => false]
     * );
     *
     * @param string $namespace
     * @param array  $filter
     * @param array  $update
     * @param array  $options
     *                ---multi 是否开启批量更新，upsert 不存在是否插入
     * @return int|null
     * @throws MongoDBException
     */
    public function updateRow(string $namespace, array $filter = [], array $update = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $result = $this->collection($namespace, $collectionOptions)->updateMany($filter, $update, $options)->getModifiedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 删除匹配到的多条数据
     *
     * @param string $namespace
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function deleteMany(string $namespace, $filter, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $result = $this->collection($namespace, $collectionOptions)->deleteMany($filter, $options)->getDeletedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 删除一条匹配的数据
     *
     * @param string $namespace
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function deleteOne(string $namespace, $filter, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $this->handleFilter($filter);
            $result = $this->collection($namespace, $collectionOptions)->deleteOne($filter, $options)->getDeletedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 通过ids删除
     *
     * @param string $namespace
     * @param array  $ids
     * @param array  $options
     * @return int
     * @throws MongoDBException
     */
    public function deleteByIds(string $namespace, array $ids, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $filter = [];
            foreach ($ids as $k => $id) {
                $ids[$k] = new ObjectId($id);
            }
            $filter['_id']['$in'] = $ids;
            $result = $this->collection($namespace, $collectionOptions)->deleteMany($filter, $options)->getDeletedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 查找集合中指定字段的不同值
     *
     * @param string $namespace
     * @param string $fieldName
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|mixed[]
     * @throws MongoDBException
     */
    public function distinct(string $namespace, string $fieldName, $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->distinct($fieldName, $filter, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 聚合查询
     *
     * @param string $namespace
     * @param array  $pipeline
     * @param array  $options
     * @param array  $collectionOptions
     * @return \Traversable
     * @throws MongoDBException
     */
    public function aggregate(string $namespace, array $pipeline = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->aggregate($pipeline, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 获取查询满足的的数量
     *
     * @param string $namespace
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function countDocuments(string $namespace, $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->countDocuments($filter, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 使用集合元数据获取集合中文档的估计数量
     *
     * @param string $namespace
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function estimatedDocumentCount(string $namespace, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->estimatedDocumentCount($options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 创建索引
     *
     * @param string $namespace
     * @param        $key
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|mixed
     * @throws MongoDBException
     */
    public function createIndex(string $namespace, $key, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->createIndex($key, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 批量创建索引
     *
     * @param string $namespace
     * @param array  $indexes
     * @param array  $options
     * @param array  $collectionOptions
     * @return string[]
     * @throws MongoDBException
     */
    public function createIndexes(string $namespace, array $indexes, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->createIndexes($indexes, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 获取所有索引信息
     *
     * @param string $namespace
     * @param array  $options
     * @param array  $collectionOptions
     * @return \MongoDB\Model\IndexInfoIterator
     * @throws MongoDBException
     */
    public function listIndexes(string $namespace, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->listIndexes($options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 移除索引
     *
     * @param string $namespace
     * @param        $indexName
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function dropIndex(string $namespace, $indexName, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->dropIndex($indexName, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 移除集合中所有的索引
     *
     * @param string $namespace
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function dropIndexes(string $namespace, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->dropIndexes($options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 创建collection
     *
     * @param string $collectionName
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function createCollection(string $collectionName, array $options = [], array $databaseOptions = [])
    {
        try {
            $isException = false;
            $result = $this->database($databaseOptions)->createCollection($collectionName, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 移除collection
     *
     * @param string $collectionName
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function dropCollection(string $collectionName, array $options = [], array $databaseOptions = [])
    {
        try {
            $isException = false;
            $result = $this->database($databaseOptions)->dropCollection($collectionName, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 修改collection
     *
     * @param string $collectionName
     * @param array  $collectionOptions
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function modifyCollection(string $collectionName, array $collectionOptions = [], array $options = [], array $databaseOptions = [])
    {
        try {
            $isException = false;
            $result = $this->database($databaseOptions)->modifyCollection($collectionName, $collectionOptions, $options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 显示所有的集合 TODO
     *
     * @param array $options
     * @param array $databaseOptions
     * @return array|\MongoDB\Model\CollectionInfoIterator
     * @throws MongoDBException
     */
    public function listCollections( array $options = [], array $databaseOptions = [])
    {
        try {
            $isException = false;
            $result = $this->database($databaseOptions)->listCollections($options);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * Executes a map-reduce aggregation on the collection.
     *
     * @param string              $namespace
     * @param JavascriptInterface $map
     * @param JavascriptInterface $reduce
     * @param                     $out
     * @param array               $options
     * @param array               $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function mapReduce(string $namespace, JavascriptInterface $map, JavascriptInterface $reduce, $out, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $data = $this->collection($namespace, $collectionOptions)->mapReduce($map, $reduce, $out, $options)->getIterator();
            $result = Arr::toArray($data);
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * Replaces at most one document matching the filter.
     *
     * @param string $namespace
     * @param        $filter
     * @param        $replacement
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null
     * @throws MongoDBException
     */
    public function replaceOne(string $namespace, $filter, $replacement, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = $this->collection($namespace, $collectionOptions)->replaceOne($filter, $replacement, $options)->getModifiedCount();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * Explains explainable commands
     *
     * @param string      $namespace
     * @param Explainable $explainable Command on which to run explain
     * @param array       $options
     * @param array       $collectionOptions
     * @return mixed
     * @throws MongoDBException
     */
    public function explain(string $namespace, Explainable $explainable, array $options = [], array $collectionOptions = [])
    {
        try {
            $isException = false;
            $result = current($this->collection($namespace, $collectionOptions)->explain($explainable, $options));
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $result;
        }
    }

    /**
     * 使用command的方式查询出数据
     *
     * @param string $namespace
     * @param array  $command
     * @return array
     * @throws Exception
     */
    public function queryByCommand(array $command)
    {
        $command = new \MongoDB\Driver\Command($command);
        return $this->mongoClient->getManager()->executeCommand($this->config['db'], $command)->toArray();
    }

    /**
     * 获取collection 中满足条件的条数
     *
     * @param string $namespace
     * @param array $filter
     * @return int
     * @throws MongoDBException
     */
    public function count(string $namespace, array $filter = [])
    {
        try {
            $isException = false;
            $command = new Command([
                'count' => $namespace,
                'query' => $filter
            ]);
            $cursor = $this->mongoClient->getManager()->executeCommand($this->config['db'], $command);
            $count = $cursor->toArray()[0]->n;
        } catch (\Exception $e) {
            $isException = true;
        } catch (Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $count;
        }
    }

    /**
     * 获取查询的个数
     *
     * @param string $namespace
     * @param array  $command
     * @return int
     * @throws Exception
     */
    public function countByCommand(array $command = [])
    {
        $command = new \MongoDB\Driver\Command($command);
        return count($this->mongoClient->getManager()->executeCommand($this->config['db'], $command)->toArray());
    }

    /**
     * @param array $command
     * @return array
     * @throws Exception
     * @throws MongoDBException
     */
    public function command(array $command = [])
    {
        try {
            $isException = false;
            $command = new Command($command);
            $res = $this->mongoClient->getManager()->executeCommand($this->config['db'], $command)->toArray();
        } catch (\Exception $e) {
            $isException = true;
        } finally {
            $this->pool->release($this);
            if ($isException) throw new MongoDBException($this->handleErrorMsg($e), 400, $e->getPrevious());
            return $res;
        }
    }

    /**
     * 判断当前的数据库连接是否已经超时
     *
     * @return bool
     * @throws \MongoDB\Driver\Exception\Exception
     * @throws MongoDBException
     */
    public function check(): bool
    {
        try {
            $command = new Command(['ping' => 1]);
            $this->mongoClient->getManager()->executeCommand($this->config['db'], $command);
            return true;
        } catch (\Throwable $e) {
            return $this->catchMongoException($e);
        }
    }

    /**
     * @param \Throwable $e
     * @return bool
     * @throws MongoDBException
     */
    private function catchMongoException(\Throwable $e)
    {
        switch ($e) {
            case ($e instanceof InvalidArgumentException):
            {
                throw new MongoDBException('mongo argument exception: ' . $e->getMessage(), 400, $e->getPrevious());
            }
            case ($e instanceof AuthenticationException):
            {
                throw new MongoDBException('mongo数据库连接授权失败:' . $e->getMessage(), 400, $e->getPrevious());
            }
            case ($e instanceof ConnectionException):
            {
                /**
                 * https://cloud.tencent.com/document/product/240/4980
                 * 存在连接失败的，那么进行重连
                 */
                for ($counts = 1; $counts <= 5; $counts++) {
                    try {
                        $this->reconnect();
                    } catch (\Exception $e) {
                        continue;
                    }
                    break;
                }
                return true;
            }
            case ($e instanceof RuntimeException):
            {
                throw new MongoDBException('mongo runtime exception: ' . $e->getMessage(), 400, $e->getPrevious());
            }
            default:
            {
                throw new MongoDBException('mongo unexpected exception: ' . $e->getMessage(), 400, $e->getPrevious());
            }
        }
    }

    /**
     * @param $filter
     * @return array
     */
    public function handleFilter(&$filter)
    {
        if (is_array($filter) && !empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            if (is_array($filter['_id'])) {
                foreach ($filter['_id'] as $k => $ids) {
                    if ($k == '$in') {
                        foreach ($ids as $k1 => $id) {
                            (!$id instanceof ObjectId) ? $filter['_id']['$in'][$k1] = new ObjectId($id) : '';
                        }
                    } elseif ($k == '$nin') {
                        foreach ($ids as $k1 => $id) {
                            (!$id instanceof ObjectId) ? $filter['_id']['$nin'][$k1] = new ObjectId($id) : '';
                        }
                    } elseif (is_numeric($k)){ // 如果是索引数字
                        (!$ids instanceof ObjectId) ? $filter['_id'][$k] = new ObjectId($ids) : '';
                    }
                }
            } else {
                $filter['_id'] = new ObjectId($filter['_id']);
            }
        }
        if (is_object($filter) && !empty($filter->_id) && !($filter->_id instanceof ObjectId)) {
            if (is_array($filter->_id)) {
                foreach ($filter->_id as $k => $ids) {
                    if ($k == '$in') {
                        foreach ($ids as $k1 => $id) {
                            (!$id instanceof ObjectId) ? $filter->_id['$in'][$k1] = new ObjectId($id) : '';
                        }
                    } elseif ($k == '$nin') {
                        foreach ($ids as $k1 => $id) {
                            (!$id instanceof ObjectId) ? $filter->_id['$nin'][$k1] = new ObjectId($id) : '';
                        }
                    } elseif (is_numeric($k)){ // 如果是索引数字
                        (!$ids instanceof ObjectId) ? $filter->_id[$k] = new ObjectId($ids) : '';
                    }
                }
            } else {
                $filter->_id = new ObjectId($filter->_id);
            }
        }
        return $filter;
    }

    /**
     * @param $e
     * @return string
     */
    public function handleErrorMsg($e)
    {
        return $e->getFile() . $e->getLine() . $e->getMessage();
    }
}
