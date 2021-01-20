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

class Dag implements Runner
{
    /**
     * @var array<Vertex>
     */
    protected $vertexes;

    /**
     * @var int
     */
    protected $concurrency;

    public function __construct(int $concurrency = 0)
    {
        $this->concurrency = $concurrency;
    }

    public function addVertex(Vertex $vertex): self
    {
        $this->vertexes[] = $vertex;
        return $this;
    }

    public function addEdge(Vertex $from, Vertex $to): self
    {
        $from->children[] = $to;
        $to->parents[] = $from;
        return $this;
    }

    public function run(): array
    {
        $all = $this->breathFirstSearchLayer();
        $results = [];

        foreach ($all as $layer) {
            $callables = [];
            foreach ($layer as $vertex) {
                $callables[$vertex->key] = function () use ($vertex, $results) {
                    return call($vertex->value, [$results]);
                };
            }
            $results = array_merge($results, \parallel($callables, $this->concurrency));
        }
        return $results;
    }

    /**
     * @return array<array<Vertex>>
     */
    private function breathFirstSearchLayer(): array
    {
        $queue = $this->buildInitialQueue();
        $all = [];
        $visited = [];

        while (! $queue->isEmpty()) {
            $length = $queue->count();
            $tmp = [];
            for ($i = 0; $i < $length; ++$i) {
                $element = $queue->dequeue();
                if (isset($visited[$element->key])) {
                    continue;
                }
                $visited[$element->key] = true;
                $tmp[] = $element;
                foreach ($element->children as $child) {
                    foreach ($child->parents as $parent) {
                        if ($parent == $element) {
                            continue;
                        }
                        if (! isset($visited[$parent->key])) {
                            continue 2;
                        }
                    }
                    $queue->enqueue($child);
                }
            }
            $all[] = $tmp;
        }
        return $all;
    }

    private function buildInitialQueue(): \SplQueue
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

        $queue = new \SplQueue();
        foreach ($roots as $root) {
            $queue->enqueue($root);
        }
        return $queue;
    }
}
