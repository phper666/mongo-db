<?php
declare(strict_types=1);
namespace Phper666\MongoDb\Command;

use Hyperf\Command\Annotation\Command;
use Hyperf\Command\Command as HyperfCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;

/**
 * @Command()
 */
class ConfigCommand extends HyperfCommand
{
    /**
     * 执行的命令行
     *
     * @var string
     */
    protected $name = 'mongodb:publish';

    public function handle()
    {
        // 从 $input 获取 config 参数
        $argument = $this->input->getOption('config');
        if ($argument) {
            $this->copySource(__DIR__ . '/../Publish/mongodb.php', BASE_PATH . '/config/autoload/mongodb.php');
            $this->line('The mongodb configuration file has been generated', 'info');
        }
    }

//    protected function getArguments()
//    {
//        return [
//            ['name', InputArgument::OPTIONAL, 'Publish the configuration for jwt-auth']
//        ];
//    }

    protected function getOptions()
    {
        return [
            ['config', NULL, InputOption::VALUE_NONE, 'Publish the configuration for mongodb']
        ];
    }

    /**
     * 复制文件到指定的目录中
     * @param $copySource
     * @param $toSource
     */
    protected function copySource($copySource, $toSource)
    {
        copy($copySource, $toSource);
    }
}
