/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * A genetic algorithm over the slice→slot assignment of one rescale, minimising the same cost as
 * {@link AntColonyPlacement} so the two are comparable as arms.
 *
 * <p>A chromosome is the slot index of each slice, and every gene must be distinct — two slices
 * cannot share a slot. Uniform crossover breaks that invariant, so offspring are repaired: duplicate
 * genes are replaced by slots the child does not use yet, taken in their stable order. Mutation
 * swaps two slices, and — where the pool offers more slots than the job needs — swaps one slice onto
 * an unused slot, which is the only move that can change *which* TaskManagers are in play at all.
 *
 * <p>The population starts from the placements the other arms would produce (identity, i.e. what
 * {@code FCFS} does, and a round-robin over TaskManagers), so the search can never do worse than the
 * simple arms on the cost function; the rest is seeded randomly for diversity.
 *
 * <p>Like the colony, this search is stateless across calls and bounded by a fixed generation count
 * rather than a clock: the adaptive scheduler asks the same question several times while evaluating
 * one rescale, and every asking must get the same answer. See {@link PlacementInstance#seed()}.
 */
final class GeneticPlacement {

    private static final int POPULATION = 24;
    private static final int GENERATIONS = 40;
    private static final int ELITES = 2;
    private static final double MUTATION_RATE = 0.2;

    private GeneticPlacement() {}

    static int[] search(PlacementInstance instance) {
        final Random random = new Random(instance.seed());
        final List<int[]> population = initialPopulation(instance, random);
        population.sort((a, b) -> Double.compare(instance.cost(a), instance.cost(b)));

        for (int generation = 0; generation < GENERATIONS; generation++) {
            final List<int[]> next = new ArrayList<>(POPULATION);
            for (int elite = 0; elite < ELITES && elite < population.size(); elite++) {
                next.add(population.get(elite));
            }
            while (next.size() < POPULATION) {
                final int[] parentA = tournament(population, instance, random);
                final int[] parentB = tournament(population, instance, random);
                final int[] child = crossover(parentA, parentB, instance, random);
                if (random.nextDouble() < MUTATION_RATE) {
                    mutate(child, instance, random);
                }
                next.add(child);
            }
            next.sort((a, b) -> Double.compare(instance.cost(a), instance.cost(b)));
            population.clear();
            population.addAll(next);
        }
        return population.get(0);
    }

    private static List<int[]> initialPopulation(PlacementInstance instance, Random random) {
        final List<int[]> population = new ArrayList<>(POPULATION);
        population.add(instance.identityPlacement());
        population.add(roundRobinPlacement(instance));
        while (population.size() < POPULATION) {
            population.add(randomPlacement(instance, random));
        }
        return population;
    }

    /**
     * The placement {@code ROUND_ROBIN} would produce, as a starting point: dealing slices over
     * TaskManagers cyclically, skipping those whose slots are already spent.
     */
    private static int[] roundRobinPlacement(PlacementInstance instance) {
        final int taskManagers = instance.taskManagerCount();
        final List<List<Integer>> slotsPerTaskManager = new ArrayList<>();
        for (int tm = 0; tm < taskManagers; tm++) {
            slotsPerTaskManager.add(new ArrayList<>());
        }
        for (int s = 0; s < instance.slotCount(); s++) {
            slotsPerTaskManager.get(instance.taskManagerOf(s)).add(s);
        }

        final int[] placement = new int[instance.groupCount()];
        final int[] used = new int[taskManagers];
        int cursor = 0;
        for (int g = 0; g < placement.length; g++) {
            for (int probe = 0; probe < taskManagers; probe++) {
                final int tm = cursor++ % taskManagers;
                if (used[tm] < slotsPerTaskManager.get(tm).size()) {
                    placement[g] = slotsPerTaskManager.get(tm).get(used[tm]++);
                    break;
                }
            }
        }
        return placement;
    }

    private static int[] randomPlacement(PlacementInstance instance, Random random) {
        final List<Integer> slots = instance.slotIndices();
        java.util.Collections.shuffle(slots, random);
        final int[] placement = new int[instance.groupCount()];
        for (int g = 0; g < placement.length; g++) {
            placement[g] = slots.get(g);
        }
        return placement;
    }

    private static int[] tournament(List<int[]> population, PlacementInstance instance, Random random) {
        final int[] a = population.get(random.nextInt(population.size()));
        final int[] b = population.get(random.nextInt(population.size()));
        return instance.cost(a) <= instance.cost(b) ? a : b;
    }

    /** Uniform crossover followed by the repair that restores "one slice per slot". */
    private static int[] crossover(
            int[] parentA, int[] parentB, PlacementInstance instance, Random random) {
        final int[] child = new int[parentA.length];
        final boolean[] taken = new boolean[instance.slotCount()];
        Arrays.fill(child, -1);

        for (int g = 0; g < child.length; g++) {
            final int gene = random.nextBoolean() ? parentA[g] : parentB[g];
            if (!taken[gene]) {
                child[g] = gene;
                taken[gene] = true;
            }
        }
        int nextFree = 0;
        for (int g = 0; g < child.length; g++) {
            if (child[g] == -1) {
                while (taken[nextFree]) {
                    nextFree++;
                }
                child[g] = nextFree;
                taken[nextFree] = true;
            }
        }
        return child;
    }

    private static void mutate(int[] placement, PlacementInstance instance, Random random) {
        if (placement.length >= 2) {
            final int i = random.nextInt(placement.length);
            final int j = random.nextInt(placement.length);
            final int swap = placement[i];
            placement[i] = placement[j];
            placement[j] = swap;
        }
        // Moving a slice onto a slot nobody uses is the only mutation that can bring another
        // TaskManager into play, which is exactly where the balance term can still improve.
        if (instance.slotCount() > placement.length) {
            final boolean[] taken = new boolean[instance.slotCount()];
            for (int slot : placement) {
                taken[slot] = true;
            }
            final List<Integer> unused = new ArrayList<>();
            for (int s = 0; s < taken.length; s++) {
                if (!taken[s]) {
                    unused.add(s);
                }
            }
            if (!unused.isEmpty()) {
                placement[random.nextInt(placement.length)] =
                        unused.get(random.nextInt(unused.size()));
            }
        }
    }
}
