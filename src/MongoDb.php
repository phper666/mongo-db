<?php
declare(strict_types=1);
namespace Phper666\Mongodb;


use Hyperf\Task\Annotation\Task;
use Phper666\Mongodb\Exception\MongoDBException;
use Phper666\Mongodb\Pool\PoolFactory;
use Hyperf\Utils\Context;

/**
 * Class MongoDb
 * @package Phper666\Mongodb
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
     * 是否自动更新时间
     *
     * @var bool
     */
    protected $timestamps = true;

    /**
     * @var PoolFactory
     */
    protected $factory;

    /**
     * @var string
     */
    protected $poolName = 'default';

    public function __construct(PoolFactory $factory)
    {
        $this->factory = $factory;
    }

    /**
     * 返回满足条件的第一个数据
     *
     * @param string $namespace
     * @param array  $filter
     * @param array  $options
     * @return array
     * @throws MongoDBException
     */
    public function fetchOne(string $namespace, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->executeQueryAll($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 返回满足filer的全部数据
     *
     * @param string $namespace
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function fetchAll(string $namespace, array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->executeQueryAll($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 使用command的方式查询出数据
     *
     * @param string $namespace
     * @param array  $command
     * @return array
     * @throws MongoDBException
     * @throws \MongoDB\Driver\Exception\Exception
     */
    public function fetchByCommand(array $command = []): array
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
     * 返回满足filer的分页数据
     *
     * @param string $namespace
     * @param int $limit
     * @param int $currentPage
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function fetchPagination(string $namespace, int $currentPage, int $limit,  array $filter = [], array $options = []): array
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->execQueryPagination($namespace, $currentPage, $limit,  $filter, $options);
        } catch (\Exception  $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 批量插入
     *
     * @param string $namespace
     * @param array $data
     * @return bool|string
     * @throws MongoDBException
     */
    public function insertAll(string $namespace, array $data)
    {
        if (count($data) == count($data, 1)) {
            throw new  MongoDBException('data is can only be a two-dimensional array');
        }

        try {
            if ($this->timestamps) {
                $time = time();
                foreach ($data as $k => &$v) {
                    $v[self::CREATED_AT] = $time;
                    $v[self::UPDATED_AT] = $time;
                }
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->insertAll($namespace, $data);
        } catch (MongoDBException $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 数据插入数据库
     *
     * @param $namespace
     * @param array $data
     * @return bool|mixed
     * @throws MongoDBException
     */
    public function insert($namespace, array $data = [])
    {
        try {
            if ($this->timestamps) {
                $time = time();
                $data[self::CREATED_AT] = $time;
                $data[self::UPDATED_AT] = $time;
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->insert($namespace, $data);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 更新数据满足$filter的行的信息成$newObject
     *
     * @param       $namespace
     * @param array $filter
     * @param array $newObj
     * @param array $options
     * @return bool
     * @throws MongoDBException
     */
    public function updateRow($namespace, array $filter = [], array $newObj = [], array $options = ['multi' => false, 'upsert' => false]): bool
    {
        try {
            if ($this->timestamps) {
                $data[self::UPDATED_AT] = time();
            }
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->updateRow($namespace, $filter, $newObj, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 删除满足条件的数据，默认只删除匹配条件的第一条记录，如果要删除多条$limit=true
     *
     * @param string $namespace
     * @param array  $filter
     * @param bool   $limit
     * @param array  $options
     * @return bool
     * @throws MongoDBException
     */
    public function delete(string $namespace, array $filter = [], bool $limit = false, array $options = []): bool
    {
        try {
            $options['limit'] = $limit;
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->delete($namespace, $filter, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 通过多个_id删除数据
     *
     * @param string $namespace
     * @param array  $ids
     * @param array  $options
     * @return bool
     * @throws MongoDBException
     */
    public function deleteByIds(string $namespace, array $ids = [], array $options = []): bool
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->deleteByIds($namespace, $ids, $options);
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        }
    }

    /**
     * 返回collection中满足条件的数量
     *
     * @param string $namespace
     * @param array $filter
     * @return bool
     * @throws MongoDBException
     */
    public function count(string $namespace, array $filter = [])
    {
        try {
            /**
             * @var $collection MongoDBConnection
             */
            $collection = $this->getConnection();
            return $collection->count($namespace, $filter);
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
     * @param array $command
     * @return array|\MongoDB\Driver\Cursor
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
     * 使用其它方法时调用这个方法会默认投递到task进程去处理，目前swoole不支持协程，会阻塞协程调度
     * @Task(timeout=30)
     * @return $this
     */
    public function mongoTask()
    {
        return $this;
    }

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
}
