<?php
declare(strict_types=1);
namespace Phper666\Mongodb;

use Hyperf\Contract\ConnectionInterface;
use MongoDB\Client;
use Phper666\Mongodb\Exception\MongoDBException;
use Hyperf\Pool\Connection;
use Hyperf\Pool\Exception\ConnectionException;
use Hyperf\Pool\Pool;
use MongoDB\BSON\ObjectId;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Exception\AuthenticationException;
use MongoDB\Driver\Exception\Exception;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Driver\Manager;
use MongoDB\Driver\Query;
use MongoDB\Driver\WriteConcern;
use Psr\Container\ContainerInterface;

class MongoDbConnection extends Connection implements ConnectionInterface
{
    /**
     * @var Manager
     */
    protected $connection;

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
            $this->connection = new Client($uri, $urlOptions, $driverOptions);
        } catch (InvalidArgumentException $e) {
            throw MongoDBException::managerError('mongodb 连接参数错误:' . $e->getMessage());
        } catch (RuntimeException $e) {
            throw MongoDBException::managerError('mongodb uri格式错误:' . $e->getMessage());
        }
        $this->lastUseTime = microtime(true);
        return true;
    }

    /**
     * Close the connection.
     */
    public function close(): bool
    {
        // TODO: Implement close() method.
        return true;
    }


    /**
     * 查询返回结果的全部数据
     *
     * @param string $namespace
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoDBException
     */
    public function executeQueryAll(string $namespace, array $filter = [], array $options = [])
    {
        if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            $filter['_id'] = new ObjectId($filter['_id']);
        }
        // 查询数据
        $result = [];
        try {
            $query = new Query($filter, $options);
            $cursor = $this->connection->executeQuery($this->config['db'] . '.' . $namespace, $query);
            foreach ($cursor as $document) {
                $document = (array)$document;
                $document['_id'] = (string)$document['_id'];
                $result[] = $document;
            }
        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        } catch (Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
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
    public function execQueryPagination(string $namespace, int $currentPage = 0, int $limit = 10, array $filter = [], array $options = [])
    {
        if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
            $filter['_id'] = new ObjectId($filter['_id']);
        }
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

        try {
            $query = new Query($filter, $options);
            $cursor = $this->connection->executeQuery($this->config['db'] . '.' . $namespace, $query);

            foreach ($cursor as $document) {
                $document = (array)$document;
                $document['_id'] = (string)$document['_id'];
                $data[] = $document;
            }

            $result['total'] = $this->count($namespace, $filter);
            $result['page_no'] = $currentPage;
            $result['page_size'] = $limit;
            $result['rows'] = $data;

        } catch (\Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        } catch (Exception $e) {
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
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
        return $this->connection->executeCommand($this->config['db'], $command)->toArray();
    }

    /**
     * 数据插入
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.insert.php
     * $data1 = ['title' => 'one'];
     * $data2 = ['_id' => 'custom ID', 'title' => 'two'];
     * $data3 = ['_id' => new MongoDB\BSON\ObjectId, 'title' => 'three'];
     *
     * @param string $namespace
     * @param array  $data
     * @param bool   $returnInsertNum
     * @return int|string
     * @throws MongoDBException
     */
    public function insert(string $namespace, array $data = [])
    {
        try {
            $bulk = new BulkWrite();
            $insertId = (string)$bulk->insert($data);
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'] . '.' . $namespace, $bulk, $written);
        } catch (\Exception $e) {
            $insertId = false;
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
            return $insertId;
        }
    }

    /**
     * 批量数据插入
     * http://php.net/manual/zh/mongodb-driver-bulkwrite.insert.php
     * $data = [
     * ['title' => 'one'],
     * ['_id' => 'custom ID', 'title' => 'two'],
     * ['_id' => new MongoDB\BSON\ObjectId, 'title' => 'three']
     * ];
     *
     * @param string $namespace
     * @param array $data
     * @return bool|string
     * @throws MongoDBException
     */
    public function insertAll(string $namespace, array $data = [])
    {
        try {
            $bulk = new BulkWrite();
            foreach ($data as $items) {
                $insertId[] = (string)$bulk->insert($items);
            }
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'] . '.' . $namespace, $bulk, $written);
        } catch (\Exception $e) {
            $insertId = false;
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
            return $insertId;
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
     * @param array  $newObj
     * @param array  $options
     *                ---multi 是否开启批量更新，upsert 不存在是否插入
     * @return bool
     * @throws MongoDBException
     */
    public function updateRow(string $namespace, array $filter = [], array $newObj = [], array $options = []): bool
    {
        try {
            if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
                $filter['_id'] = new ObjectId($filter['_id']);
            }

            $bulk = new BulkWrite;
            $bulk->update(
                $filter,
                ['$set' => $newObj],
                $options
            );
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $result = $this->connection->executeBulkWrite($this->config['db'] . '.' . $namespace, $bulk, $written);
            $modifiedCount = $result->getModifiedCount();
            $update = $modifiedCount == 0 ? false : true;
        } catch (\Exception $e) {
            $update = false;
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
            return $update;
        }
    }

    /**
     * 删除数据
     *
     * @param string $namespace
     * @param array  $filter
     * @param array  $options
     * @return bool
     * @throws MongoDBException
     */
    public function delete(string $namespace, array $filter = [], array $options = []): bool
    {
        try {
            if (!empty($filter['_id']) && !($filter['_id'] instanceof ObjectId)) {
                $filter['_id'] = new ObjectId($filter['_id']);
            }
            $bulk = new BulkWrite;
            $bulk->delete($filter, $options);
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'] . '.' . $namespace, $bulk, $written);
            $delete = true;
        } catch (\Exception $e) {
            $delete = false;
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
            return $delete;
        }
    }

    /**
     * 通过ids删除
     *
     * @param string $namespace
     * @param array  $ids
     * @param array  $options
     * @return bool
     * @throws MongoDBException
     */
    public function deleteByIds(string $namespace, array $ids, array $options = [])
    {
        try {
            $filter = [];
            foreach ($ids as $k => $id) {
                $filter[] = ['_id' => new ObjectId($id)];
            }
            $filter = ['$or' => $filter];
            if (empty($filter['$or'])) return false;
            $bulk = new BulkWrite();
            $bulk->delete($filter, $options);
            $written = new WriteConcern(WriteConcern::MAJORITY, 1000);
            $this->connection->executeBulkWrite($this->config['db'] . '.' . $namespace, $bulk, $written);
            $delete = true;
        } catch (\Exception $e) {
            $delete = false;
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
            return $delete;
        }
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
            $command = new Command([
                'count' => $namespace,
                'query' => $filter
            ]);
            $cursor = $this->connection->executeCommand($this->config['db'], $command);
            $count = $cursor->toArray()[0]->n;
            return $count;
        } catch (\Exception $e) {
            $count = 0;
            throw new MongoDBException($this->handleErrorMsg($e));
        } catch (Exception $e) {
            $count = 0;
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
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
        return count($this->connection->executeCommand($this->config['db'], $command)->toArray());
    }

    /**
     * @param array $command
     * @return array|\MongoDB\Driver\Cursor
     * @throws Exception
     * @throws MongoDBException
     */
    public function command(array $command = [])
    {
        try {
            $command = new Command($command);
            $res = $this->connection->executeCommand($this->config['db'], $command);
        } catch (\Exception $e) {
            $res = [];
            throw new MongoDBException($this->handleErrorMsg($e));
        } finally {
            $this->pool->release($this);
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
            $this->connection->executeCommand($this->config['db'], $command);
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
                    throw MongoDBException::managerError('mongo argument exception: ' . $e->getMessage());
                }
            case ($e instanceof AuthenticationException):
                {
                    throw MongoDBException::managerError('mongo数据库连接授权失败:' . $e->getMessage());
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
                    throw MongoDBException::managerError('mongo runtime exception: ' . $e->getMessage());
                }
            default:
                {
                    throw MongoDBException::managerError('mongo unexpected exception: ' . $e->getMessage());
                }
        }
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
