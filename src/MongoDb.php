<?php
declare(strict_types=1);
namespace Phper666\MongoDb;

use Hyperf\Task\Annotation\Task;
use MongoDB\BSON\JavascriptInterface;
use MongoDB\Operation\Explainable;
use Phper666\MongoDb\Exception\MongoDBException;
use Phper666\MongoDb\Pool\PoolFactory;
use Hyperf\Utils\Context;
use Phper666\MongoDb\Utils\Arr;

/**
 * Class MongoDb
 */
class MongoDb
{
    /**
     * The name of the "created at" column.
     *
     * @var string
     */
    const CREATED_AT = 'created_at';

    /**
     * The name of the "updated at" column.
     *
     * @var string
     */
    const UPDATED_AT = 'updated_at';

    /**
     * mongodb表
     * @var string
     */
    public $collectionName = null;

    /**
     * 是否自动更新时间
     *
     * @var bool
     */
    public $timestamps = true;

    /**
     * @var PoolFactory
     */
    protected $factory;

    /**
     * @var string
     */
    protected $poolName = 'default';

    /**
     * MongoDb constructor.
     * @param PoolFactory $factory
     * @throws MongoDBException
     */
    public function __construct(PoolFactory $factory)
    {
        $this->factory = $factory;
    }

    /**
     * @param $collectionName
     */
    public function setCollectionName($collectionName)
    {
        $this->collectionName = $collectionName;
        return $this;
    }

    /**
     * @param bool $bool
     */
    public function setTimestamps(bool $bool)
    {
        $this->timestamps = $bool;
        return $this;
    }

    /**
     * 返回满足条件的第一个数据
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param array  $options
     * @return array
     * @throws MongoDBException
     */
    public function findOne($filter = [], array $options = [], array $collectionOptions = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->findOne($this->collectionName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 返回满足filer的全部数据
     *
     * @Task(timeout=30)
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function findAll(array $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->findAll($this->collectionName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 返回满足filer的分页数据
     *
     * @Task(timeout=30)
     * @param int    $currentPage
     * @param int    $limit
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function findPagination(int $currentPage, int $limit,  array $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->findPagination($this->collectionName, $currentPage, $limit,  $filter, $options, $collectionOptions);
        } catch (\Exception  $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 使用command的方式查询出数据
     *
     * @Task(timeout=30)
     * @param array $command
     * @return array
     * @throws MongoDBException
     * @throws \MongoDB\Driver\Exception\Exception
     */
    public function findByCommand(array $command = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->queryByCommand($command);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 查找单个文档并删除它，返回原始文档
     *
     * @Task(timeout=30)
     * @param       $filter
     * @param array $options
     * @param array $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function findOneAndDelete($filter, array $options = [], array $collectionOptions = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->findOneAndDelete($this->collectionName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 查找单个文档并替换它，返回原始文档或替换文件
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param        $replacement
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|null
     * @throws MongoDBException
     */
    public function findOneAndReplace($filter, $replacement, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->findOneAndReplace($this->collectionName, $filter, $replacement, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 查找单个文档并更新它，返回原始文档或更新后的文件
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object|null
     * @throws MongoDBException
     */
    public function findOneAndUpdate($filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->findOneAndUpdate($this->collectionName, $filter, $update, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 插入多个数据
     *
     * @Task(timeout=30)
     * @param array $documents
     * @param array $options
     * @param array $collectionOptions
     * @return bool|mixed[]
     * @throws MongoDBException
     */
    public function insertMany(array $documents = [], array $options = [], array $collectionOptions = [])
    {
        try {
            if ($this->timestamps) {
                $time = time();
                foreach ($documents as &$document) {
                    $document[self::CREATED_AT] = $time;
                    $document[self::UPDATED_AT] = $time;
                }
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->insertMany($this->collectionName, $documents, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 插入一个数据
     *
     * @Task(timeout=30)
     * @param array $document
     * @param array $options
     * @param array $collectionOptions
     * @return mixed
     * @throws MongoDBException
     */
    public function insertOne($document = [], array $options = [], array $collectionOptions = [])
    {
        try {
            if ($this->timestamps) {
                $time = time();
                $document[self::CREATED_AT] = $time;
                $document[self::UPDATED_AT] = $time;
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->insertOne($this->collectionName, $document, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 更新匹配的到的所有数据
     *
     * @Task(timeout=30)
     * @param       $filter
     * @param       $update
     * @param array $options
     * @param array $collectionOptions
     * @return int|null
     * @throws MongoDBException
     */
    public function updateMany($filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            if ($this->timestamps) {
                $update['$set'][self::UPDATED_AT] = time();
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->updateMany($this->collectionName, $filter, $update, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 更新匹配的到的一条数据
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param        $update
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null
     * @throws MongoDBException
     */
    public function updateOne($filter, $update, array $options = [], array $collectionOptions = [])
    {
        try {
            if ($this->timestamps) {
                $update['$set'][self::UPDATED_AT] = time();
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->updateOne($this->collectionName, $filter, $update, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject
     *
     * @Task(timeout=30)
     * @param array $filter
     * @param array $update
     * @param array $options
     * @return int|null
     * @throws MongoDBException
     */
    public function updateRow(array $filter = [], array $update = [], array $options = ['multi' => false, 'upsert' => false])
    {
        try {
            if ($this->timestamps) {
                $update['$set'][self::UPDATED_AT] = time();
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->updateRow($this->collectionName, $filter, $update, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 删除匹配到的多条数据
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function deleteMany($filter, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->deleteMany($this->collectionName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 删除一条匹配的数据
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function deleteOne($filter, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->deleteOne($this->collectionName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 通过多个_id删除数据
     *
     * @Task(timeout=30)
     * @param array $ids
     * @param array $options
     * @return int
     * @throws MongoDBException
     */
    public function deleteByIds(array $ids = [], array $options = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->deleteByIds($this->collectionName, $ids, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 查找集合中指定字段的不同值
     *
     * @Task(timeout=30)
     * @param string $fieldName
     * @param array  $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|mixed[]
     * @throws MongoDBException
     */
    public function distinct(string $fieldName, $filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->distinct($this->collectionName, $fieldName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 聚合查询
     *
     * @Task(timeout=30)
     * @param array $pipeline
     * @param array $options
     * @param array $collectionOptions
     * @return \Traversable
     * @throws MongoDBException
     */
    public function aggregate(array $pipeline = [], array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->aggregate($this->collectionName, $pipeline, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 获取查询满足的的数量
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function countDocuments($filter = [], array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->countDocuments($this->collectionName, $filter, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 使用集合元数据获取集合中文档的估计数量
     *
     * @Task(timeout=30)
     * @param array  $options
     * @param array  $collectionOptions
     * @return int
     * @throws MongoDBException
     */
    public function estimatedDocumentCount(array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->estimatedDocumentCount($this->collectionName, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 创建索引
     *
     * @Task(timeout=30)
     * @param        $key
     * @param array  $options
     * @param array  $collectionOptions
     * @return mixed
     * @throws MongoDBException
     */
    public function createIndex($key, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->createIndex($this->collectionName, $key, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 批量创建索引
     *
     * @Task(timeout=30)
     * @param array  $indexes
     * @param array  $options
     * @param array  $collectionOptions
     * @return string[]
     * @throws MongoDBException
     */
    public function createIndexes(array $indexes, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->createIndexes($this->collectionName, $indexes, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 获取所有索引信息
     *
     * @Task(timeout=30)
     * @param array $options
     * @param array $collectionOptions
     * @return \MongoDB\Model\IndexInfoIterator
     * @throws MongoDBException
     */
    public function listIndexes(array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->listIndexes($this->collectionName, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 移除索引
     *
     * @Task(timeout=30)
     * @param        $indexName
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function dropIndex($indexName, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->dropIndex($this->collectionName, $indexName, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 移除集合中所有的索引
     *
     * @Task(timeout=30)
     * @param array  $options
     * @param array  $collectionOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function dropIndexes(array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->dropIndexes($this->collectionName, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * Executes a map-reduce aggregation on the collection.
     *
     * @Task(timeout=30)
     * @param JavascriptInterface $map
     * @param JavascriptInterface $reduce
     * @param                     $out
     * @param array               $options
     * @param array               $collectionOptions
     * @return array
     * @throws MongoDBException
     */
    public function mapReduce(JavascriptInterface $map, JavascriptInterface $reduce, $out, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            $data = $collection->mapReduce($this->collectionName, $map, $reduce, $out, $options, $collectionOptions);
            return Arr::toArray($data);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * Replaces at most one document matching the filter.
     *
     * @Task(timeout=30)
     * @param        $filter
     * @param        $replacement
     * @param array  $options
     * @param array  $collectionOptions
     * @return int|null
     * @throws MongoDBException
     */
    public function replaceOne($filter, $replacement, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->replaceOne($this->collectionName, $filter, $replacement, $options, $collectionOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * Explains explainable commands
     *
     * @Task(timeout=30)
     * @param Explainable $explainable Command on which to run explain
     * @param array       $options
     * @param array       $collectionOptions
     * @return mixed
     * @throws MongoDBException
     */
    public function explain(Explainable $explainable, array $options = [], array $collectionOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return current($collection->explain($this->collectionName, $explainable, $options, $collectionOptions));
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 创建集合
     *
     * @Task(timeout=30)
     * @param string $collectionName
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function createCollection(array $options = [], array $databaseOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->createCollection($this->collectionName, $options, $databaseOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 移除集合
     *
     * @Task(timeout=30)
     * @param string $collectionName
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function dropCollection(array $options = [], array $databaseOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->dropCollection($this->collectionName, $options, $databaseOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 修改集合
     *
     * @Task(timeout=30)
     * @param string $collectionName
     * @param array  $collectionOptions
     * @param array  $options
     * @param array  $databaseOptions
     * @return array|object
     * @throws MongoDBException
     */
    public function modifyCollection(array $collectionOptions = [], array $options = [], array $databaseOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->modifyCollection($this->collectionName, $collectionOptions, $options, $databaseOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 显示所有的集合 TODO
     *
     * @Task(timeout=30)
     * @param array $options
     * @param array $databaseOptions
     * @return array|\MongoDB\Model\CollectionInfoIterator
     * @throws MongoDBException
     */
    public function listCollections(array $options = [], array $databaseOptions = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->listCollections($options, $databaseOptions);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 使用command的方式获取个数
     *
     * @param array $command
     * @return int
     * @throws MongoDBException
     * @throws \MongoDB\Driver\Exception\Exception
     */
    public function countByCommand(array $command = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->countByCommand($command);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 聚合查询
     *
     * @Task(timeout=30)
     * @param array $command
     * @return array
     * @throws MongoDBException
     * @throws \MongoDB\Driver\Exception\Exception
     */
    public function command(array $command = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->command($command);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * @return mixed|null
     */
    private function getConnection()
    {
        $connection = null;
        $hasContextConnection = Context::has($this->getContextKey());
        if ($hasContextConnection) {
            $connection = Context::get($this->getContextKey());
        }
        if (!$connection instanceof MongoDbConnection) {
            $pool = $this->factory->getPool($this->poolName);
            $connection = $pool->get()->getConnection();
        }
        return $connection;
    }

    /**
     * The key to identify the connection object in coroutine context.
     */
    private function getContextKey(): string
    {
        return sprintf('mongodb.connection.%s', $this->poolName);
    }

    /**
     * @param $e
     * @return string
     */
    private function handleErrorMsg($e)
    {
        return $e->getFile() . $e->getLine() . $e->getMessage();
    }

    public function __set($name, $value)
    {
        $this->$name = $value;
        return $this;
    }
}
