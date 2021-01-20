<?php

declare(strict_types=1);
/**
 * This file is part of Hyperf.
 *
 * @link     https://www.hyperf.io
 * @document https://hyperf.wiki
 * @contact  group@hyperf.io
 * @license  https://github.com/hyperf/hyperf/blob/master/LICENSE
 */
namespace Hyperf\Dag;

use Hyperf\Dag\Exception\InvalidArgumentException;
use Hyperf\Engine\Channel;
use Hyperf\Utils\Coroutine;
use Hyperf\Utils\Coroutine\Concurrent;

class Dag implements Runner
{
    /**
     * @var array<Vertex>
     */
    protected $vertexes = [];

    /**
     * @var int
     */
    protected $concurrency = 100;

    /**
     * Add a vertex to the dag.
     * It doesn't make sense to add a vertex with the same key more than once.
     * If so they are simply ignored.
     */
    public function addVertex(Vertex $vertex): self
    {
        foreach ($this->vertexes as $added) {
            if ($added->key == $vertex->key) {
                return $this;
            }
        }
        $this->vertexes[] = $vertex;
        return $this;
    }

    /**
     * Add an edge to the DAG.
     */
    public function addEdge(Vertex $from, Vertex $to): self
    {
        $from->children[] = $to;
        $to->parents[] = $from;
        return $this;
    }

    /**
     * Run the DAG.
     */
    public function run(): array
    {
        $queue = new Channel(1);
        Coroutine::create(function () use ($queue) {
            $this->buildInitialQueue($queue);
        });

        $total = count($this->vertexes);
        $visited = [];
        $results = [];
        $concurrent = new Concurrent($this->concurrency);

        while (count($visited) < $total) {
            $element = $queue->pop();
            if (isset($visited[$element->key])) {
                continue;
            }
            // this channel will be closed after the completion of the corresponding task.
            $visited[$element->key] = new Channel();
            $concurrent->create(function () use ($queue, $visited, $element, &$results) {
                $results[$element->key] = call($element->value, [$results]);
                $visited[$element->key]->close();
                Coroutine::create(function () use ($element, $queue, $visited) {
                    $this->scheduleChildren($element, $queue, $visited);
                });
            });
        }
        // wait for all pending tasks to resolve
        foreach ($visited as $element) {
            $element->pop();
        }
        return $results;
    }

    public function getConcurrency(): int
    {
        return $this->concurrency;
    }

    public function setConcurrency(int $concurrency): self
    {
        $this->concurrency = $concurrency;
        return $this;
    }

    private function scheduleChildren(Vertex $element, Channel $queue, array $visited): void
    {
        foreach ($element->children as $child) {
            // Only schedule child if all parents but this one is complete
            foreach ($child->parents as $parent) {
                if ($parent->key == $element->key) {
                    continue;
                }
                if (! isset($visited[$parent->key])) {
                    continue 2;
                }
                // Parent might be running. Wait until completion.
                $visited[$parent->key]->pop();
            }
            $queue->push($child);
        }
    }

    private function buildInitialQueue(Channel $queue): void
    {
        $roots = [];
        /** @var Vertex $vertex */
        foreach ($this->vertexes as $vertex) {
            if (empty($vertex->parents)) {
                $roots[] = $vertex;
            }
        }

        if (empty($roots)) {
            throw new InvalidArgumentException('no roots can be found in dag');
        }

        foreach ($roots as $root) {
            $queue->push($root);
        }
    }
}
