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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.ThesisSlotAssigner.Strategy;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ThesisSlotAssigner}, the thesis fork's placement strategies.
 *
 * <p>What matters here is not that every slice gets a slot — the default assigner already does that
 * — but <em>which TaskManager</em> each slice lands on, since that is the decision Li et al. (IEEE
 * TBD 2023) identify as left to chance in stock Flink. The tests therefore assert on the spread of
 * slices over TaskManagers, plus the determinism the adaptive scheduler's speculative rescale
 * evaluation requires.
 */
class ThesisSlotAssignerTest {

    private static final String TM_A = "tm-a";
    private static final String TM_B = "tm-b";
    private static final String TM_C = "tm-c";

    @AfterEach
    void clearStrategy() {
        System.clearProperty("thesis.assign.strategy");
        System.clearProperty("thesis.arm.file");
        System.clearProperty("thesis.loads.file");
        System.clearProperty("thesis.speeds.file");
        ThesisSlotAssigner.forgetPublishedArm();
        ThesisSlotAssigner.forgetPublishedValues();
    }

    /**
     * The Phase-1 observation: with the stock iteration order, both slices of a parallelism-2 job
     * land on the same TaskManager, because the free slots arrive grouped per TM. This is the
     * "before" the strategies are meant to correct.
     */
    @Test
    void fcfsFollowsIterationOrderAndPacksOneTaskManager() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final Map<String, Integer> spread = spreadOf(assign(Strategy.FCFS, 2, freeSlots));

        assertThat(spread).isEqualTo(Collections.singletonMap(TM_A, 2));
    }

    /** The Phase-2 fix: the same job now spreads its two slices over two TaskManagers. */
    @Test
    void roundRobinSpreadsSlicesOverDistinctTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final Map<String, Integer> spread = spreadOf(assign(Strategy.ROUND_ROBIN, 2, freeSlots));

        assertThat(spread).containsOnlyKeys(TM_A, TM_B).containsValues(1, 1);
    }

    /** With more slices than TaskManagers, round-robin keeps dealing in laps. */
    @Test
    void roundRobinDealsCyclicallyWhenSlicesExceedTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);

        final List<String> placement = placementOf(assign(Strategy.ROUND_ROBIN, 5, freeSlots));

        assertThat(placement).containsExactly(TM_A, TM_B, TM_C, TM_A, TM_B);
    }

    /**
     * A TaskManager that runs out of free slots drops out of the rotation instead of stalling the
     * assignment, so an unbalanced supply of slots still gets fully used.
     */
    @Test
    void roundRobinSkipsExhaustedTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 1, TM_B, 3);

        final List<String> placement = placementOf(assign(Strategy.ROUND_ROBIN, 4, freeSlots));

        assertThat(placement).containsExactly(TM_A, TM_B, TM_B, TM_B);
    }

    /**
     * Least-loaded reads "loaded" off the free capacity a TaskManager still offers: a TM holding
     * fewer free slots is already busier, so it receives fewer slices.
     */
    @Test
    void leastLoadedPrefersTaskManagerWithMostFreeCapacity() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 3, TM_B, 1);

        final Map<String, Integer> spread = spreadOf(assign(Strategy.LEAST_LOADED, 4, freeSlots));

        assertThat(spread).containsEntry(TM_A, 3).containsEntry(TM_B, 1);
    }

    /** With homogeneous TaskManagers, least-loaded degenerates into round-robin. */
    @Test
    void leastLoadedMatchesRoundRobinOnHomogeneousTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final List<String> placement = placementOf(assign(Strategy.LEAST_LOADED, 4, freeSlots));

        assertThat(placement).containsExactly(TM_A, TM_B, TM_A, TM_B);
    }

    // ------------------------------------------------------------------------
    // LPT — the load-aware greedy, and the only arm that reads TaskManager speeds
    // ------------------------------------------------------------------------

    /**
     * The heterogeneity case, and the reason the arm exists. Four equal slices over two
     * TaskManagers where one is three times the machine: a fast TM should take three of them.
     *
     * <p>{@code LEAST_LOADED} splits them 2/2 on the same input, because it balances free SLOTS and
     * has no idea the machines differ — which is the gap the offline bench measured at 9-14%.
     */
    @Test
    void lptGivesTheFasterTaskManagerMoreSlices(@TempDir Path tempDir) throws IOException {
        publishSpeeds(tempDir, TM_A, 3.0, TM_B, 1.0);
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 4, TM_B, 4);

        assertThat(spreadOf(assign(Strategy.LPT, 4, freeSlots)))
                .containsEntry(TM_A, 3)
                .containsEntry(TM_B, 1);
        assertThat(spreadOf(assign(Strategy.LEAST_LOADED, 4, freeSlots)))
                .as("LEAST_LOADED balances slot counts, so it cannot see the difference")
                .containsEntry(TM_A, 2)
                .containsEntry(TM_B, 2);
    }

    /**
     * The unequal-slices case. A vertex pinned narrower than the rest makes the first slices
     * expensive and the rest cheap; LPT must place the expensive ones first and therefore split
     * them, where iteration order packs both onto one TaskManager.
     *
     * <p>This is the arrangement that cost 45% of throughput in the 2026-08-17 cluster runs.
     */
    @Test
    void lptSplitsTheExpensiveSlicesInsteadOfPackingThem(@TempDir Path tempDir) throws IOException {
        final JobVertexID wide = new JobVertexID();
        final JobVertexID narrow = new JobVertexID();
        // Slices 0 and 1 carry both vertices (1 + 3), slices 2 and 3 only the wide one (1).
        publishLoads(tempDir, wide, 1.0, narrow, 3.0);
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        final List<String> lpt =
                placementOf(assignStaggered(Strategy.LPT, freeSlots, wide, 4, narrow, 2));
        assertThat(lpt)
                .as("the two expensive slices must not share a TaskManager")
                .satisfies(placement -> assertThat(placement.get(0)).isNotEqualTo(placement.get(1)));

        final List<String> fcfs =
                placementOf(assignStaggered(Strategy.FCFS, freeSlots, wide, 4, narrow, 2));
        assertThat(fcfs.get(0)).as("iteration order packs them").isEqualTo(fcfs.get(1));
    }

    /** With nothing published every slice weighs 1.0 and every machine is nominal: an even split. */
    @Test
    void lptWithoutPublishedFilesSplitsEvenly() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        assertThat(spreadOf(assign(Strategy.LPT, 4, freeSlots)))
                .containsEntry(TM_A, 2)
                .containsEntry(TM_B, 2);
    }

    /** A TaskManager missing from the speeds file is nominal, not unusable. */
    @Test
    void lptTreatsUnlistedTaskManagersAsNominal(@TempDir Path tempDir) throws IOException {
        publishSpeeds(tempDir, TM_A, 3.0);
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 4, TM_B, 4);

        assertThat(spreadOf(assign(Strategy.LPT, 4, freeSlots)))
                .containsEntry(TM_A, 3)
                .containsEntry(TM_B, 1);
    }

    /**
     * The adaptive scheduler calls the assigner speculatively while evaluating candidate rescalings,
     * so the same inputs in a different iteration order must produce the same placement. This is
     * what the internal ordering of slices and slots buys.
     */
    @Test
    void placementIsIndependentOfFreeSlotIterationOrder() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);
        final List<SlotInfo> shuffled = new ArrayList<>(freeSlots);
        Collections.reverse(shuffled);

        for (Strategy strategy :
                new Strategy[] {Strategy.ROUND_ROBIN, Strategy.LEAST_LOADED, Strategy.LPT}) {
            assertThat(placementOf(assign(strategy, 4, shuffled)))
                    .as("placement under %s must not depend on slot iteration order", strategy)
                    .isEqualTo(placementOf(assign(strategy, 4, freeSlots)));
        }
    }

    /**
     * STOCK must reproduce unpatched Flink, which on a first submission (no previous allocations)
     * means {@link DefaultSlotAssigner} — the same packing the DEFAULT arm produces.
     */
    @Test
    void stockMatchesDefaultSlotAssignerWithoutPreviousAllocations() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2);

        assertThat(placementOf(assign(Strategy.STOCK, 2, freeSlots)))
                .isEqualTo(placementOf(assign(Strategy.FCFS, 2, freeSlots)));
    }

    /** STOCK places every slice, like any other arm. */
    @Test
    void stockAssignsEverySlice() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);

        assertThat(assign(Strategy.STOCK, 4, freeSlots)).hasSize(4);
    }

    /** An unknown strategy name must not take the JobManager down; it falls back to the default. */
    @Test
    void unknownStrategyFallsBackToRoundRobin() {
        System.setProperty("thesis.assign.strategy", "not-a-strategy");

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.ROUND_ROBIN);
    }

    @Test
    void strategyIsReadCaseInsensitively() {
        System.setProperty("thesis.assign.strategy", "least_loaded");

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.LEAST_LOADED);
    }

    /** Phase-2 experiments named this arm DEFAULT; the name must keep working. */
    @Test
    void legacyDefaultNameStillSelectsFcfs() {
        System.setProperty("thesis.assign.strategy", "DEFAULT");

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.FCFS);
    }

    // ------------------------------------------------------------------------
    // the search arms — ACO and GA
    // ------------------------------------------------------------------------

    /** Whatever the search decides, it must still be a valid assignment: one slice, one slot. */
    @Test
    void searchArmsGiveEverySliceItsOwnSlot() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);

        for (Strategy strategy : new Strategy[] {Strategy.ACO, Strategy.GA}) {
            final Collection<SlotAssignment> assignments = assign(strategy, 4, freeSlots);

            assertThat(assignments).as("%s places every slice", strategy).hasSize(4);
            assertThat(assignments.stream().map(a -> a.getSlotInfo().getAllocationId()))
                    .as("%s never puts two slices in one slot", strategy)
                    .doesNotHaveDuplicates();
        }
    }

    /**
     * The searches are randomised, and the adaptive scheduler asks the same question repeatedly
     * while evaluating one rescale. Seeding them from the instance is what keeps the answer stable —
     * without it the placement the scheduler decided on could differ from the one it executes.
     */
    @Test
    void searchArmsAreDeterministicAndOrderIndependent() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);
        final List<SlotInfo> shuffled = new ArrayList<>(freeSlots);
        Collections.reverse(shuffled);

        for (Strategy strategy : new Strategy[] {Strategy.ACO, Strategy.GA}) {
            final JobVertexID vertexId = new JobVertexID();
            final List<String> first = placementOf(assign(strategy, 4, freeSlots, vertexId));

            assertThat(placementOf(assign(strategy, 4, freeSlots, vertexId)))
                    .as("%s repeated on the same instance", strategy)
                    .isEqualTo(first);
            assertThat(placementOf(assign(strategy, 4, shuffled, vertexId)))
                    .as("%s must not depend on slot iteration order", strategy)
                    .isEqualTo(first);
        }
    }

    /**
     * The point of paying for a search: with no state to preserve, the only cost term left is
     * balance, so both metaheuristics must spread at least as well as the greedy arms — and strictly
     * better than FCFS, which packs the first TaskManager.
     */
    @Test
    void searchArmsSpreadSlicesOverAllTaskManagers() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 2, TM_B, 2, TM_C, 2);

        assertThat(spreadOf(assign(Strategy.FCFS, 3, freeSlots))).hasSizeLessThan(3);
        for (Strategy strategy : new Strategy[] {Strategy.ACO, Strategy.GA}) {
            assertThat(spreadOf(assign(strategy, 3, freeSlots)))
                    .as("%s uses every TaskManager", strategy)
                    .containsOnlyKeys(TM_A, TM_B, TM_C);
        }
    }

    /** An unbalanced supply of slots is still fully usable by the searches. */
    @Test
    void searchArmsCopeWithUnevenSlotSupply() {
        final List<SlotInfo> freeSlots = slotsOf(TM_A, 1, TM_B, 3);

        for (Strategy strategy : new Strategy[] {Strategy.ACO, Strategy.GA}) {
            final Map<String, Integer> spread = spreadOf(assign(strategy, 4, freeSlots));

            assertThat(spread).as("%s", strategy).containsEntry(TM_A, 1).containsEntry(TM_B, 3);
        }
    }

    // ------------------------------------------------------------------------
    // the arm file — how the external meta-scheduler publishes its choice
    // ------------------------------------------------------------------------

    /** The published arm takes over from the pod's configured default. */
    @Test
    void armFileSelectsTheStrategy(@TempDir Path tempDir) throws IOException {
        useArmFile(publishArm(tempDir, "LEAST_LOADED"));

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.LEAST_LOADED);
    }

    /**
     * The point of Phase 3: the meta-scheduler changes the arm between two rescales of the same run,
     * with no JobManager restart. (The read is throttled in production, so the test drops the cache
     * instead of waiting out the interval.)
     */
    @Test
    void republishedArmIsPickedUpWithoutRestart(@TempDir Path tempDir) throws IOException {
        final Path armFile = publishArm(tempDir, "ROUND_ROBIN");
        useArmFile(armFile);
        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.ROUND_ROBIN);

        Files.write(armFile, "STOCK\n".getBytes(StandardCharsets.UTF_8));
        ThesisSlotAssigner.expirePublishedArm();

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.STOCK);
    }

    /** No published arm at all is the normal state of a fixed-arm experiment, not a failure. */
    @Test
    void missingArmFileFallsBackToTheConfiguredStrategy(@TempDir Path tempDir) {
        useArmFile(tempDir.resolve("never-published"));

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.ROUND_ROBIN);
    }

    /**
     * The meta-scheduler rewrites the file while the JobManager may be reading it, so a truncated or
     * garbled read must not throw and must not silently switch arms — the last good value stands.
     */
    @Test
    void garbledArmFileKeepsTheLastGoodArm(@TempDir Path tempDir) throws IOException {
        final Path armFile = publishArm(tempDir, "LEAST_LOADED");
        useArmFile(armFile);
        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.LEAST_LOADED);

        Files.write(armFile, "LEAST_LO".getBytes(StandardCharsets.UTF_8));
        ThesisSlotAssigner.expirePublishedArm();

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.LEAST_LOADED);
    }

    /** A manual override still wins, so a fixed-arm run cannot be hijacked by a stale arm file. */
    @Test
    void explicitStrategyOverridesThePublishedArm(@TempDir Path tempDir) throws IOException {
        useArmFile(publishArm(tempDir, "LEAST_LOADED"));
        System.setProperty("thesis.assign.strategy", "STOCK");

        assertThat(ThesisSlotAssigner.currentStrategy()).isEqualTo(Strategy.STOCK);
    }

    // ------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------

    /** Writes an arm file the way the meta-scheduler does: one strategy name, trailing newline. */
    private static Path publishArm(Path directory, String strategy) throws IOException {
        final Path armFile = directory.resolve("arm");
        Files.write(armFile, (strategy + "\n").getBytes(StandardCharsets.UTF_8));
        return armFile;
    }

    private static void useArmFile(Path armFile) {
        System.clearProperty("thesis.assign.strategy");
        System.setProperty("thesis.arm.file", armFile.toString());
        ThesisSlotAssigner.forgetPublishedArm();
    }

    /**
     * Runs the assigner over a two-vertex job where the second is pinned to a lower parallelism, so
     * the slices carry unequal load — the only shape in which a load-aware arm can differ from a
     * count-based one.
     */
    private static Collection<SlotAssignment> assignStaggered(
            Strategy strategy,
            Collection<? extends SlotInfo> freeSlots,
            JobVertexID wide,
            int wideParallelism,
            JobVertexID narrow,
            int narrowParallelism) {
        System.setProperty("thesis.assign.strategy", strategy.name());

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        final List<TestVertexInformation> vertices = new ArrayList<>();
        vertices.add(new TestVertexInformation(wide, wideParallelism, slotSharingGroup));
        vertices.add(new TestVertexInformation(narrow, narrowParallelism, slotSharingGroup));

        final Map<JobVertexID, Integer> parallelismForVertices = new HashMap<>();
        parallelismForVertices.put(wide, wideParallelism);
        parallelismForVertices.put(narrow, narrowParallelism);

        return new ThesisSlotAssigner()
                .assignSlots(
                        new TestJobInformation(vertices),
                        freeSlots,
                        new VertexParallelism(parallelismForVertices),
                        JobAllocationsInformation.empty());
    }

    /** Publish per-vertex costs the way scripts/publish-loads.sh does. */
    private static void publishLoads(Path dir, Object... vertexAndCost) throws IOException {
        final StringBuilder contents = new StringBuilder();
        for (int i = 0; i < vertexAndCost.length; i += 2) {
            contents.append(((JobVertexID) vertexAndCost[i]).toHexString())
                    .append(' ')
                    .append(vertexAndCost[i + 1])
                    .append('\n');
        }
        publish(dir, "loads", "thesis.loads.file", contents.toString());
    }

    /** Publish per-TaskManager speeds the way scripts/publish-speeds.sh does. */
    private static void publishSpeeds(Path dir, Object... taskManagerAndSpeed) throws IOException {
        final StringBuilder contents = new StringBuilder();
        for (int i = 0; i < taskManagerAndSpeed.length; i += 2) {
            contents.append(taskManagerAndSpeed[i])
                    .append(' ')
                    .append(taskManagerAndSpeed[i + 1])
                    .append('\n');
        }
        publish(dir, "speeds", "thesis.speeds.file", contents.toString());
    }

    private static void publish(Path dir, String name, String property, String contents)
            throws IOException {
        final Path file = dir.resolve(name);
        Files.write(file, contents.getBytes(StandardCharsets.UTF_8));
        System.setProperty(property, file.toString());
        // The reader caches for a second, and these tests write faster than that.
        ThesisSlotAssigner.forgetPublishedValues();
    }

    /** Runs the assigner over a single-vertex job of the given parallelism. */
    private static Collection<SlotAssignment> assign(
            Strategy strategy, int parallelism, Collection<? extends SlotInfo> freeSlots) {
        return assign(strategy, parallelism, freeSlots, new JobVertexID());
    }

    /**
     * The same run with an explicit vertex id. The search arms seed themselves from the instance,
     * the job vertex included, so comparing two invocations only makes sense when they describe the
     * same vertex — which is what the real scheduler does when it re-asks about one rescale.
     */
    private static Collection<SlotAssignment> assign(
            Strategy strategy,
            int parallelism,
            Collection<? extends SlotInfo> freeSlots,
            JobVertexID vertexId) {
        System.setProperty("thesis.assign.strategy", strategy.name());

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        final JobInformation jobInformation =
                new TestJobInformation(
                        Collections.singletonList(
                                new TestVertexInformation(
                                        vertexId, parallelism, slotSharingGroup)));

        final Map<JobVertexID, Integer> parallelismForVertices = new HashMap<>();
        parallelismForVertices.put(vertexId, parallelism);

        return new ThesisSlotAssigner()
                .assignSlots(
                        jobInformation,
                        freeSlots,
                        new VertexParallelism(parallelismForVertices),
                        JobAllocationsInformation.empty());
    }

    /** The TaskManager each slice landed on, in slice order. */
    private static List<String> placementOf(Collection<SlotAssignment> assignments) {
        final List<String> placement = new ArrayList<>();
        for (SlotAssignment assignment : assignments) {
            placement.add(taskManagerIdOf(assignment.getSlotInfo()));
        }
        return placement;
    }

    /** How many slices each TaskManager received. */
    private static Map<String, Integer> spreadOf(Collection<SlotAssignment> assignments) {
        final Map<String, Integer> spread = new LinkedHashMap<>();
        for (String taskManager : placementOf(assignments)) {
            spread.merge(taskManager, 1, Integer::sum);
        }
        return spread;
    }

    private static String taskManagerIdOf(SlotInfo slot) {
        return slot.getTaskManagerLocation().getResourceID().getResourceIdString();
    }

    /**
     * Builds free slots from {@code (taskManagerId, slotCount)} pairs, emitted grouped per
     * TaskManager — the order in which the real SlotPool tends to offer them, and the order that
     * makes the default assigner pack a single TM.
     */
    private static List<SlotInfo> slotsOf(Object... taskManagerAndCount) {
        final List<SlotInfo> slots = new ArrayList<>();
        for (int i = 0; i < taskManagerAndCount.length; i += 2) {
            final String taskManagerId = (String) taskManagerAndCount[i];
            final int count = (Integer) taskManagerAndCount[i + 1];
            final TaskManagerLocation location =
                    new TaskManagerLocation(
                            new ResourceID(taskManagerId), InetAddress.getLoopbackAddress(), 42);
            for (int slotNumber = 0; slotNumber < count; slotNumber++) {
                slots.add(new FixedLocationSlotInfo(location, slotNumber));
            }
        }
        return slots;
    }

    /**
     * A {@link SlotInfo} pinned to one TaskManager. {@link TestSlotInfo} cannot be used here: it
     * hands out a freshly generated location on every call, so slots could never be grouped by TM.
     */
    private static final class FixedLocationSlotInfo implements SlotInfo {
        private final AllocationID allocationId = new AllocationID();
        private final TaskManagerLocation taskManagerLocation;
        private final int physicalSlotNumber;

        private FixedLocationSlotInfo(
                TaskManagerLocation taskManagerLocation, int physicalSlotNumber) {
            this.taskManagerLocation = taskManagerLocation;
            this.physicalSlotNumber = physicalSlotNumber;
        }

        @Override
        public AllocationID getAllocationId() {
            return allocationId;
        }

        @Override
        public TaskManagerLocation getTaskManagerLocation() {
            return taskManagerLocation;
        }

        @Override
        public int getPhysicalSlotNumber() {
            return physicalSlotNumber;
        }

        @Override
        public ResourceProfile getResourceProfile() {
            return ResourceProfile.ANY;
        }

        @Override
        public boolean willBeOccupiedIndefinitely() {
            return false;
        }
    }
}
