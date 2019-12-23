<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://doc.hyperf.io
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */

namespace Phper666\MongoDb\Command\Migrations;

use Hyperf\Command\Annotation\Command;
use Hyperf\Command\ConfirmableTrait;
use Hyperf\Di\Annotation\Inject;
use Phper666\MongoDb\MongoDbConnection;
use Symfony\Component\Console\Input\InputOption;
/**
 * @Command()
 */
class MigrationCommand extends BaseCommand
{
    use ConfirmableTrait;

    protected $name = 'mongodb:migration';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Run the mongodb migrations';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        if (! $this->confirmToProceed()) {
            return;
        }

        $this->setOutput($this->output)->runMigration($this->getMigrationPaths(), []);

        if ($this->input->getOption('seed') && ! $this->input->getOption('pretend')) {
            $this->call('mongodb:seed', ['--force' => true]);
        }
    }

    protected function getOptions(): array
    {
        return [
            ['force', null, InputOption::VALUE_NONE, 'Force the operation to run when in production'],
            ['path', null, InputOption::VALUE_OPTIONAL, 'The path to the migrations files to be executed'],
            ['realpath', null, InputOption::VALUE_NONE, 'Indicate any provided migration file paths are pre-resolved absolute paths'],
            ['seed', null, InputOption::VALUE_NONE, 'Indicates if the seed task should be re-run']
        ];
    }

}
