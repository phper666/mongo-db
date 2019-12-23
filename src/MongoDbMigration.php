<?php
declare(strict_types=1);
namespace Phper666\MongoDb;

use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ContainerInterface;
use Hyperf\Di\Annotation\Inject;
use MongoDB\BSON\JavascriptInterface;
use MongoDB\BSON\ObjectId;
use MongoDB\Operation\Explainable;
use Phper666\MongoDb\Exception\MongoDBException;
use Phper666\MongoDb\Pool\PoolFactory;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Database;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use Hyperf\Utils\ApplicationContext;
/**
 * Class MongoDbMigration
 */
class MongoDbMigration
{
    /**
     * @Inject()
     * @var PoolFactory
     */
    protected $factory;

    /**
     * @var string
     */
    protected $poolName = 'default';

    /**
     * @var array
     */
    protected $config;

    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var Client
     */
    protected $mongoClient;

    /**
     * MongoDbConnection constructor.
     * @throws MongoDBException
     */
    public function __construct()
    {
        $this->container = ApplicationContext::getContainer();
        $config = $this->container->get(ConfigInterface::class);
        $this->config = $config->get('mongodb.' . $this->poolName);
        $this->getConnection();
    }

    /**
     * @param string $collectionName
     * @param array  $collectionOptions
     * @return Collection
     */
    protected function collection(string $collectionName, array $collectionOptions = [])
    {
        $collection = make(Collection::class, [$this->mongoClient->getManager(), $this->config['db'], $collectionName, $collectionOptions]);
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
     * 返回满足条件的第一个数据
     *
     * @param string $collectionName
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function findOne(string $collectionName, $filter = [], array $options = [], array $collectionOptions = []): array
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->findOne($filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 返回满足filer的全部数据
     *
     * @param string $collectionName
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return \MongoDB\Driver\Cursor
     * @throws MongoDBException
     */
    public function findAll(string $collectionName, array $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->find($filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 插入多个数据
     *
     * @param string $collectionName
     * @param array  $documents
     * @param array  $options
     * @param array  $collectionOptions
     * @return mixed[]|string
     */
    public function insertMany(string $collectionName, array $documents = [], array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->insertMany($documents, $options)->getInsertedIds();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 插入一个数据
     *
     * @param string $collectionName
     * @param array  $documents
     * @param array  $options
     * @param array  $collectionOptions
     * @return mixed|string
     */
    public function insertOne(string $collectionName, $document = [], array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->insertOne($document, $options)->getInsertedId();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 更新匹配的到的所有数据
     *
     * @param string $collectionName
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null|bool|string
     */
    public function updateMany(string $collectionName, $filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            $this->handleFilter($filter);
            return $this->collection($collectionName, $collectionOptions)->updateMany($filter, $update, $options)->getModifiedCount();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 更新匹配的到的一条数据
     *
     * @param string $collectionName
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null|bool|string
     */
    public function updateOne(string $collectionName, $filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            $this->handleFilter($filter);
            return $this->collection($collectionName, $collectionOptions)->updateOne($filter, $update, $options)->getModifiedCount();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
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
     * @param string $collectionName
     * @param array  $filter
     * @param array  $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null|string
     */
    public function updateRow(string $collectionName, array $filter = [], array $update = [], array $options = [], array $collectionOptions = [])
    {
        try {
            $this->handleFilter($filter);
            return $this->collection($collectionName, $collectionOptions)->updateMany($filter, $update, $options)->getModifiedCount();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 删除匹配到的多条数据
     *
     * @param string $collectionName
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|bool|string
     */
    public function deleteMany(string $collectionName, $filter, array $options = [], array $collectionOptions = [])
    {
        try {
            $this->handleFilter($filter);
            return $this->collection($collectionName, $collectionOptions)->deleteMany($filter, $options)->getDeletedCount();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 删除一条匹配的数据
     *
     * @param string $collectionName
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|bool|string
     */
    public function deleteOne(string $collectionName, $filter, array $options = [], array $collectionOptions = [])
    {
        try {
            $this->handleFilter($filter);
            return $this->collection($collectionName, $collectionOptions)->deleteOne($filter, $options)->getDeletedCount();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 通过ids删除
     * @param string $collectionName
     * @param array  $ids
     * @param array  $options
     * @param array  $collectionOptions
     * @return bool|int|string
     */
    public function deleteByIds(string $collectionName, array $ids, array $options = [], array $collectionOptions = [])
    {
        try {
            if (empty($ids)) return false;
            $filter = [];
            foreach ($ids as $k => $id) {
                $ids[$k] = new ObjectId($id);
            }
            $filter['_id']['$in'] = $ids;
            return $this->collection($collectionName, $collectionOptions)->deleteMany($filter, $options)->getDeletedCount();
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 判断是否已经存在索引
     *
     * @param string $collectionName
     * @param        $key
     * @return bool
     */
    public function isExistIndex(string $collectionName, $key)
    {
        foreach ($this->listIndexes($collectionName) as $index) {
            $keys = array_keys($index->getKey());
            if (is_array($key) && empty(array_diff($keys, $key))) {
                return true;
            }

            if (is_string($key) && in_array($key, $keys)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 创建索引
     *
     * @param string $collectionName
     * @param        $key
     * @param array  $options
     * @param array  $collectionOptions
     * @return string
     * @throws MongoDBException
     */
    public function createIndex(string $collectionName, $key, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->createIndex($key, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 批量创建索引
     *
     * @param string $collectionName
     * @param array  $indexes
     * @param array  $options
     * @param array  $collectionOptions
     * @return string|string[]
     */
    public function createIndexes(string $collectionName, array $indexes, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->createIndexes($indexes, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 获取所有索引信息
     *
     * @param string $collectionName
     * @param array  $options
     * @param array  $collectionOptions
     * @return \MongoDB\Model\IndexInfoIterator|string
     */
    public function listIndexes(string $collectionName, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->listIndexes($options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 移除索引
     *
     * @param string $collectionName
     * @param        $indexName
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|string
     */
    public function dropIndex(string $collectionName, $indexName, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->dropIndex($indexName, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 移除集合中所有的索引
     *
     * @param string $collectionName
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|string
     */
    public function dropIndexes(string $collectionName, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->dropIndexes($options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * Executes a map-reduce aggregation on the collection.
     *
     * @param string              $collectionName
     * @param JavascriptInterface $map
     * @param JavascriptInterface $reduce
     * @param                     $out
     * @param array               $options
     * @param array               $collectionOptions
     * @return \MongoDB\MapReduceResult|string
     */
    public function mapReduce(string $collectionName, JavascriptInterface $map, JavascriptInterface $reduce, $out, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->mapReduce($map, $reduce, $out, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * Replaces at most one document matching the filter.
     *
     * @param string $collectionName
     * @param        $filter
     * @param        $replacement
     * @param array  $options
     * @param array  $collectionOptions
     * @return \MongoDB\UpdateResult|string
     */
    public function replaceOne(string $collectionName, $filter, $replacement, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->replaceOne($filter, $replacement, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * Explains explainable commands
     *
     * @param string      $collectionName
     * @param Explainable $explainable
     * @param array       $options
     * @param array       $collectionOptions
     * @return array|object|string
     */
    public function explain(string $collectionName, Explainable $explainable, array $options = [], array $collectionOptions = [])
    {
        try {
            return $this->collection($collectionName, $collectionOptions)->explain($explainable, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 判断是否已经存在表
     *
     * @param string $collectionName
     * @return bool
     */
    public function isExistCollection(string $collectionName)
    {
        $collections = [];
        foreach ($this->listCollections() as $collectionInfo) {
            $collections[] = $collectionInfo->getName();
        }
        if (array_keys($collections, $collectionName)) {
            return true;
        }

        return false;
    }

    /**
     * 创建集合
     *
     * @param string $collectionName
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object|string
     */
    public function createCollection(string $collectionName, array $options = [], array $databaseOptions = [])
    {
        try {
            return $this->database($databaseOptions)->createCollection($collectionName, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 移除集合
     *
     * @param string $collectionName
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object|string
     */
    public function dropCollection(string $collectionName, array $options = [], array $databaseOptions = [])
    {
        try {
            return $this->database($databaseOptions)->dropCollection($collectionName, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 修改集合
     *
     * @param string $collectionName
     * @param array  $collectionOptions
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object|string
     */
    public function modifyCollection(string $collectionName, array $collectionOptions = [], array $options = [], array $databaseOptions = [])
    {
        try {
            return $this->database($databaseOptions)->modifyCollection($collectionName, $collectionOptions, $options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * 显示所有的集合
     *
     * @param array $options
     * @param array $databaseOptions
     * @return \MongoDB\Model\CollectionInfoIterator|string
     */
    public function listCollections(array $options = [], array $databaseOptions = [])
    {
        try {
            return $this->database($databaseOptions)->listCollections($options);
        } catch (\Exception $e) {
            return $this->handleErrorMsg($e);
        }
    }

    /**
     * @throws MongoDBException
     */
    private function getConnection()
    {
        try {
            /**
             * http://php.net/manual/zh/mongodb-driver-manager.construct.php
             */
            $username = $this->config['username'];
            $password = $this->config['password'];
            $driverOptions = $this->config['driver_options'];
            if (!empty($username) && !empty($password)) {
                $uri = sprintf(
                    'mongodb://%s:%s@%s:%d/%s',
                    $username,
                    $password,
                    $this->config['host'],
                    $this->config['port'],
                    $this->config['db']
                );
            } else {
                $uri = sprintf(
                    'mongodb://%s:%d/%s',
                    $this->config['host'],
                    $this->config['port'],
                    $this->config['db']
                );
            }
            $urlOptions = [];
            //数据集
            $replica = isset($this->config['replica']) ? $this->config['replica'] : null;
            if ($replica) {
                $urlOptions['replicaSet'] = $replica;
            }
            $this->mongoClient = new Client($uri, $urlOptions, $driverOptions);
        } catch (InvalidArgumentException $e) {
            throw MongoDBException::managerError('mongodb 连接参数错误:' . $e->getMessage());
        } catch (RuntimeException $e) {
            throw MongoDBException::managerError('mongodb uri格式错误:' . $e->getMessage());
        }
        $this->lastUseTime = microtime(true);
        return $this;
    }

    /**
     * @param $filter
     * @return array
     */
    private function handleFilter(&$filter)
    {
        if (is_array($filter) && !empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            $filter['_id'] = new ObjectId($filter['_id']);
        }
        if (is_object($filter) && !empty($filter->_id) && !($filter->_id instanceof ObjectId)) {
            $filter->_id = new ObjectId($filter->_id);
        }
        return $filter;
    }

    /**
     * @param $e
     * @return string
     */
    private function handleErrorMsg($e)
    {
        return $e->getFile() . PHP_EOL .$e->getLine(). PHP_EOL . $e->getMessage();
    }
}
