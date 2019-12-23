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

use Hyperf\Command\Command;
use Hyperf\Contract\ConfigInterface;
use Hyperf\Contract\ContainerInterface;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Utils\Str;
use Hyperf\Utils\Collection;
use Symfony\Component\Console\Output\OutputInterface;
abstract class BaseCommand extends Command
{
    /**
     * @Inject()
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var string
     */
    protected $poolName = 'default';

    /**
     * @var array
     */
    protected $paths = [];

    /**
     * The output interface implementation.
     *
     * @var OutputInterface
     */
    protected $output;

    /**
     * The filesystem instance.
     *
     * @Inject()
     * @var \Hyperf\Utils\Filesystem\Filesystem
     */
    protected $files;

    public function runMigration($paths = [], array $options = [])
    {
        $this->notes = [];
        $migrations = [];

        $files = $this->getMigrationFiles($paths);

        foreach ($files as $file) {
            $this->files->requireOnce($file);
            $class = $this->getMigrationName($file);
            $migrations[] = $class;
            $migration = new $class();
            try {
                $data = $migration->up();
                $this->output->write($class . ':' . json_encode($data) . PHP_EOL);
            }catch (\Exception $exception) {
                $data = $migration->down();
                $this->output->write($class . ':' . json_encode($data) . PHP_EOL);
            }
        }

        return $migrations;
    }

    /**
     * Require in all the migration files in a given path.
     */
    public function requireFiles(array $files): void
    {
        foreach ($files as $file) {
            $this->files->requireOnce($file);
        }
    }

    /**
     * Set the output implementation that should be used by the console.
     *
     * @return $this
     */
    public function setOutput(OutputInterface $output)
    {
        $this->output = $output;

        return $this;
    }

    /**
     * Get all of the migration paths.
     */
    protected function getMigrationPaths(): array
    {
        // Here, we will check to see if a path option has been defined. If it has we will
        // use the path relative to the root of the installation folder so our database
        // migrations may be run for any customized path from within the application.
        if ($this->input->hasOption('path') && $this->input->getOption('path')) {
            return collect($this->input->getOption('path'))->map(function ($path) {
                return ! $this->usingRealPath()
                    ? BASE_PATH . DIRECTORY_SEPARATOR . $path
                    : $path;
            })->all();
        }

        return array_merge(
            $this->paths(),
            [$this->getMigrationPath()]
        );
    }

    /**
     * Determine if the given path(s) are pre-resolved "real" paths.
     *
     * @return bool
     */
    protected function usingRealPath()
    {
        return $this->input->hasOption('realpath') && $this->input->getOption('realpath');
    }

    /**
     * Get the path to the migration directory.
     *
     * @return string
     */
    protected function getMigrationPath()
    {
        $defaultPath = BASE_PATH . DIRECTORY_SEPARATOR . 'migrations' . DIRECTORY_SEPARATOR . 'mongodb';
        $path = $this->getConfig()['migration']['path'] ?? $defaultPath;
        return $path;
    }

    /**
     * @return mixed
     */
    public function getConfig()
    {
        $config = $this->container->get(ConfigInterface::class);
        return $config->get('mongodb.' . $this->poolName);
    }

    /**
     * Get all of the migration files in a given path.
     *
     * @param array|string $paths
     */
    public function getMigrationFiles($paths): array
    {
        return Collection::make($paths)->flatMap(function ($path) {
            return Str::endsWith($path, '.php') ? [$path] : $this->files->glob($path . '/*.php');
        })->filter()->sortBy(function ($file) {
            return $this->getMigrationName($file);
        })->values()->keyBy(function ($file) {
            return $this->getMigrationName($file);
        })->all();
    }

    /**
     * Get the name of the migration.
     */
    public function getMigrationName(string $path): string
    {
        return str_replace('.php', '', basename($path));
    }

    /**
     * Register a custom migration path.
     */
    public function path(string $path): void
    {
        $this->paths = array_unique(array_merge($this->paths, [$path]));
    }

    /**
     * Get all of the custom migration paths.
     */
    public function paths(): array
    {
        return $this->paths;
    }

    /**
     * Write a note to the conosle's output.
     *
     * @param string $message
     */
    protected function note($message)
    {
        if ($this->output) {
            $this->output->writeln($message);
        }
    }
}
