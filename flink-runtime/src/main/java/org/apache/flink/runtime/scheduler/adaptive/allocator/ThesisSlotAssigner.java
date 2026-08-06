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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thesis {@link SlotAssigner} — Phase 2 (real placement strategies).
 *
 * <p>Where {@link DefaultSlotAssigner} pairs each execution slot sharing group with whatever slot
 * comes next in iteration order — the arbitrary placement criticised by Li et al., "Cost-Efficient
 * Scheduling of Streaming Applications in Apache Flink on Cloud" (IEEE TBD 2023) — this assigner
 * picks the target TaskManager explicitly, following a selectable strategy:
 *
 * <ul>
 *   <li>{@code FCFS} — iteration order unconditionally; the paper's "arbitrary placement" in
 *       isolation, and the Phase-1 reproduction. Formerly called {@code DEFAULT}.
 *   <li>{@code ROUND_ROBIN} — deal the slices over the TaskManagers cyclically, so consecutive
 *       slices land on different TMs.
 *   <li>{@code LEAST_LOADED} — place each slice on the TaskManager with the most free capacity
 *       left. With homogeneous TMs this converges to round-robin; it diverges when TMs offer
 *       unequal numbers of free slots (a partial rescale, or slots held by another job).
 *   <li>{@code ACO} / {@code GA} — metaheuristics over the whole instance, minimising the
 *       balance-versus-state-locality cost in {@link PlacementInstance}. These are the only arms
 *       that weigh one objective against another instead of maximising a single one.
 *   <li>{@code STOCK} — delegates to the assigner unpatched Flink would have used, which is the
 *       experimental baseline.
 * </ul>
 *
 * <p><b>Which arm is the baseline.</b> {@code STOCK}, not {@code DEFAULT}. Stock Flink picks its
 * assigner per call: {@link DefaultSlotAssigner} only when there are no previous allocations, i.e.
 * on a first submission, and {@link StateLocalitySlotAssigner} otherwise. Since a rescale always
 * carries previous allocations, unpatched Flink anchors slices to the slots already holding their
 * state — so comparing a rebalancing arm against {@code DEFAULT} would measure it against something
 * Flink never does at a rescale.
 *
 * <p>Enablement is unchanged from Phase 1: the {@code THESIS_SLOT_ASSIGNER} environment variable (or
 * the {@code thesis.slot.assigner} system property) routes placement here, so this class stays inert
 * unless explicitly turned on.
 *
 * <p><b>Where the arm comes from (Phase 3).</b> The strategy in force is re-read on every assignment
 * round from an <em>arm file</em> — {@code /var/thesis/arm} by default, overridable with {@code
 * THESIS_ARM_FILE} — which the external meta-scheduler rewrites whenever it changes its mind. That
 * makes the arm a live signal rather than a process-level setting: Phase 2 could only change strategy
 * by restarting the JobManager, which is incompatible with learning across the rescale events of a
 * single run. The file is consulted at most once per {@value #ARM_REFRESH_MILLIS} ms and any read
 * problem leaves the previous value standing, so a missing or half-written file can never fail a
 * scheduling decision. {@code THESIS_ASSIGN_STRATEGY} still works and is what the arm file falls back
 * to, which keeps the Phase-2 fixed-arm experiments reproducible.
 *
 * <p><b>Purity.</b> The adaptive scheduler invokes this assigner speculatively while evaluating
 * candidate rescalings, so it must not mutate any state, and equivalent inputs must yield equivalent
 * placements. To guarantee the latter it imposes a total order on both the slices and the slots
 * before deciding, instead of relying on the incidental iteration order of the caller's collections.
 * All learning (bandit / SARSA) stays outside the JobManager, in the Kubernetes-side meta-scheduler;
 * {@link #currentStrategy()} is the seam through which that external arm signal gets injected in
 * Phase 3.
 *
 * <p>Note: like Phase 1, and unlike {@link StateLocalitySlotAssigner}, this assigner ignores {@code
 * previousAllocations} (state locality). Placement is decided purely by the strategy, which is what
 * makes the strategy's effect measurable in isolation.
 */
public class ThesisSlotAssigner implements SlotAssigner {

    private static final Logger LOG = LoggerFactory.getLogger(ThesisSlotAssigner.class);

    private static final String ENV_FLAG = "THESIS_SLOT_ASSIGNER";
    private static final String PROP_FLAG = "thesis.slot.assigner";

    private static final String ENV_STRATEGY = "THESIS_ASSIGN_STRATEGY";
    private static final String PROP_STRATEGY = "thesis.assign.strategy";

    private static final String ENV_ARM_FILE = "THESIS_ARM_FILE";
    private static final String PROP_ARM_FILE = "thesis.arm.file";
    private static final String DEFAULT_ARM_FILE = "/var/thesis/arm";

    /**
     * How long a value read from the arm file is reused before the file is consulted again. The
     * adaptive scheduler calls the assigner several times while evaluating one rescale, so an
     * unthrottled read would put a file system call on the JobManager's main thread per candidate;
     * it would also let the arm change <em>within</em> a single evaluation, making the placement it
     * decided on differ from the one it later executes.
     */
    private static final long ARM_REFRESH_MILLIS = 1000;

    /** Placement strategies — the arms the external meta-scheduler will eventually select from. */
    public enum Strategy {
        /**
         * Delegates to whichever assigner unpatched Flink would have used: {@link
         * DefaultSlotAssigner} on a first submission, {@link StateLocalitySlotAssigner} on a
         * rescale. This — not {@link #DEFAULT} — is the honest experimental baseline, because at a
         * rescale {@code previousAllocations} is never empty and stock Flink therefore anchors each
         * slice to the slot already holding its state.
         */
        STOCK,
        /**
         * First come, first served: each slice takes the next slot in iteration order, i.e. {@link
         * DefaultSlotAssigner} unconditionally. Stock Flink only behaves this way on a first
         * submission, so this arm is the paper's "arbitrary placement" in isolation — useful as a
         * contrast, but NOT the baseline at a rescale. Accepted under its old name {@code DEFAULT}.
         */
        FCFS,
        /** Cyclic over TaskManagers, so consecutive slices land on different TMs. */
        ROUND_ROBIN,
        /** Each slice goes to the TaskManager with the most free capacity remaining. */
        LEAST_LOADED,
        /**
         * Ant Colony Optimisation over the balance-vs-state-locality cost in {@link
         * PlacementInstance}. Unlike the three arms above it optimises a trade-off rather than a
         * single objective, which is what gives the meta-scheduler something to choose between.
         */
        ACO,
        /** A genetic algorithm over the same cost as {@link #ACO}. */
        GA
    }

    /** Old name of {@link Strategy#FCFS}, still accepted so Phase-2 runs stay reproducible. */
    private static final String LEGACY_FCFS_NAME = "DEFAULT";

    private static final String ENV_BALANCE_WEIGHT = "THESIS_COST_BALANCE";
    private static final String ENV_LOCALITY_WEIGHT = "THESIS_COST_LOCALITY";

    private static final Strategy DEFAULT_STRATEGY = Strategy.ROUND_ROBIN;

    /**
     * Whether the thesis slot assigner should replace the default one. Checks the system property
     * first, then the environment variable.
     */
    public static boolean isEnabled() {
        final String prop = System.getProperty(PROP_FLAG);
        if (prop != null) {
            return isTruthy(prop);
        }
        final String env = System.getenv(ENV_FLAG);
        return env != null && isTruthy(env);
    }

    private static boolean isTruthy(String value) {
        final String v = value.trim();
        return v.equals("1") || v.equalsIgnoreCase("true") || v.equalsIgnoreCase("yes");
    }

    /**
     * The strategy to apply on this invocation — the single point where placement is parameterised,
     * and therefore the seam through which the external meta-scheduler injects its arm.
     *
     * <p>Precedence, most specific first: the {@code thesis.assign.strategy} system property (a
     * manual override, and what the tests use), then the arm file the meta-scheduler publishes, then
     * the {@code THESIS_ASSIGN_STRATEGY} environment variable baked into the pod, then {@link
     * #DEFAULT_STRATEGY}. Anything unparseable is ignored in favour of the next source down, so a
     * garbled signal degrades to the pod's configured arm rather than to an exception.
     *
     * <p>It stays read-only by design: the assigner may consult the signal, never update it. All
     * learning happens outside the JobManager.
     */
    static Strategy currentStrategy() {
        final Strategy override = parseStrategy(System.getProperty(PROP_STRATEGY), PROP_STRATEGY);
        if (override != null) {
            return override;
        }
        final Strategy published = strategyFromArmFile();
        if (published != null) {
            return published;
        }
        final Strategy configured = parseStrategy(System.getenv(ENV_STRATEGY), ENV_STRATEGY);
        return configured != null ? configured : DEFAULT_STRATEGY;
    }

    @Nullable
    private static Strategy parseStrategy(@Nullable String raw, String source) {
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }
        final String name = raw.trim().toUpperCase();
        if (LEGACY_FCFS_NAME.equals(name)) {
            return Strategy.FCFS;
        }
        try {
            return Strategy.valueOf(name);
        } catch (IllegalArgumentException e) {
            LOG.warn("[THESIS_ARM] unknown strategy '{}' from {}, ignoring it", raw.trim(), source);
            return null;
        }
    }

    /** The last value read from the arm file, and when it was read. */
    private static final AtomicReference<PublishedArm> PUBLISHED_ARM = new AtomicReference<>(null);

    /**
     * The arm published by the external meta-scheduler, or {@code null} if it has not published one
     * — the file being absent is the normal state of a fixed-arm experiment, not an error.
     *
     * <p>The cached value is deliberately kept when a read fails: the meta-scheduler rewrites the
     * file while the JobManager may be reading it, so a truncated or momentarily missing file is an
     * expected race, and reverting to another arm mid-run would silently corrupt the very reward
     * attribution the learning depends on.
     */
    @Nullable
    private static Strategy strategyFromArmFile() {
        final Path armFile = armFilePath();
        final PublishedArm cached = PUBLISHED_ARM.get();
        final long now = System.currentTimeMillis();
        if (cached != null
                && cached.path.equals(armFile)
                && now - cached.readAtMillis < ARM_REFRESH_MILLIS) {
            return cached.strategy;
        }

        Strategy strategy = cached == null ? null : cached.strategy;
        try {
            if (Files.isReadable(armFile)) {
                final String contents =
                        new String(Files.readAllBytes(armFile), StandardCharsets.UTF_8);
                final Strategy parsed = parseStrategy(contents, armFile.toString());
                if (parsed != null) {
                    strategy = parsed;
                }
            }
        } catch (IOException | RuntimeException e) {
            LOG.warn("[THESIS_ARM] could not read arm file {}, keeping {}", armFile, strategy, e);
        }

        PUBLISHED_ARM.set(new PublishedArm(armFile, strategy, now));
        if (cached != null && cached.strategy != strategy) {
            LOG.info(
                    "[THESIS_ARM] arm changed {} -> {} (published in {})",
                    cached.strategy,
                    strategy,
                    armFile);
        }
        return strategy;
    }

    private static Path armFilePath() {
        String configured = System.getProperty(PROP_ARM_FILE);
        if (configured == null) {
            configured = System.getenv(ENV_ARM_FILE);
        }
        if (configured == null || configured.trim().isEmpty()) {
            configured = DEFAULT_ARM_FILE;
        }
        return Paths.get(configured.trim());
    }

    /**
     * Ages the cached arm so a test sees a rewritten file without waiting out the refresh interval.
     * The cached value itself is preserved, because keeping it is exactly the behaviour under test
     * when the rewritten file turns out to be unreadable.
     */
    @VisibleForTesting
    static void expirePublishedArm() {
        final PublishedArm cached = PUBLISHED_ARM.get();
        if (cached != null) {
            PUBLISHED_ARM.set(new PublishedArm(cached.path, cached.strategy, 0L));
        }
    }

    /** Forgets the published arm entirely, so one test cannot leak its arm file into the next. */
    @VisibleForTesting
    static void forgetPublishedArm() {
        PUBLISHED_ARM.set(null);
    }

    /** An arm read from the arm file, with the path it came from and the time it was read. */
    private static final class PublishedArm {
        private final Path path;
        @Nullable private final Strategy strategy;
        private final long readAtMillis;

        private PublishedArm(Path path, @Nullable Strategy strategy, long readAtMillis) {
            this.path = path;
            this.strategy = strategy;
            this.readAtMillis = readAtMillis;
        }
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {

        final Strategy strategy = currentStrategy();

        // STOCK hands the untouched arguments to the very class stock Flink would have picked, so
        // the baseline is Flink's real behaviour rather than a reimplementation of it. Handled
        // before the groups are built below, since the delegate builds its own.
        if (strategy == Strategy.STOCK) {
            final SlotAssigner delegate =
                    previousAllocations.isEmpty()
                            ? new DefaultSlotAssigner()
                            : new StateLocalitySlotAssigner();
            final Collection<SlotAssignment> stockAssignments =
                    delegate.assignSlots(
                            jobInformation, freeSlots, vertexParallelism, previousAllocations);
            logPlacement(strategy, stockAssignments, freeSlots, delegate.getClass());
            return stockAssignments;
        }

        // Build the execution slot sharing groups exactly like DefaultSlotAssigner does. Each group
        // is one vertical slice of the pipeline (one subtask of every operator in the sharing
        // group), and is the unit this assigner places onto a slot.
        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(
                    DefaultSlotAssigner.createExecutionSlotSharingGroups(
                            vertexParallelism, slotSharingGroup));
        }
        // The groups come out of a hash map; order them by subtask index so that "slice i" means
        // the same thing on every (possibly speculative) invocation.
        allGroups.sort(Comparator.comparingInt(ThesisSlotAssigner::subtaskIndexOf));

        final Collection<SlotAssignment> assignments;
        switch (strategy) {
            case FCFS:
                assignments = assignInIterationOrder(allGroups, freeSlots);
                break;
            case ROUND_ROBIN:
                assignments = assignRoundRobin(allGroups, groupSlotsByTaskManager(freeSlots));
                break;
            case LEAST_LOADED:
                assignments = assignLeastLoaded(allGroups, groupSlotsByTaskManager(freeSlots));
                break;
            case ACO:
            case GA:
                assignments =
                        assignBySearch(
                                strategy, allGroups, freeSlots, jobInformation, previousAllocations);
                break;
            default:
                throw new IllegalStateException("Unhandled strategy " + strategy);
        }

        logPlacement(strategy, assignments, freeSlots, null);
        return assignments;
    }

    /** Baseline: whatever slot comes next, i.e. verbatim {@link DefaultSlotAssigner}. */
    private static Collection<SlotAssignment> assignInIterationOrder(
            List<ExecutionSlotSharingGroup> groups, Collection<? extends SlotInfo> freeSlots) {
        final Iterator<? extends SlotInfo> iterator = freeSlots.iterator();
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : groups) {
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }

    /**
     * Deal the slices cyclically over the TaskManagers. A TM that runs out of free slots drops out
     * of the rotation, so an unbalanced supply of slots still gets fully used.
     */
    private static Collection<SlotAssignment> assignRoundRobin(
            List<ExecutionSlotSharingGroup> groups, List<TaskManagerSlots> taskManagers) {
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        int cursor = 0;
        for (ExecutionSlotSharingGroup group : groups) {
            TaskManagerSlots target = null;
            // At most one full lap: if no TM has capacity we are out of slots altogether.
            for (int probe = 0; probe < taskManagers.size(); probe++) {
                final TaskManagerSlots candidate = taskManagers.get(cursor % taskManagers.size());
                cursor++;
                if (candidate.hasFreeSlot()) {
                    target = candidate;
                    break;
                }
            }
            assignments.add(new SlotAssignment(takeSlot(target, group), group));
        }
        return assignments;
    }

    /**
     * Place each slice on the TaskManager with the most free capacity left — the load-balancing arm.
     * Ties fall to the first TaskManager in the (stable) ordering, so the outcome is deterministic.
     */
    private static Collection<SlotAssignment> assignLeastLoaded(
            List<ExecutionSlotSharingGroup> groups, List<TaskManagerSlots> taskManagers) {
        final Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : groups) {
            TaskManagerSlots target = null;
            for (TaskManagerSlots candidate : taskManagers) {
                if (candidate.hasFreeSlot()
                        && (target == null || candidate.remaining() > target.remaining())) {
                    target = candidate;
                }
            }
            assignments.add(new SlotAssignment(takeSlot(target, group), group));
        }
        return assignments;
    }

    /**
     * The metaheuristic arms: hand the whole instance to a search that minimises the balance /
     * state-locality cost, then read the placement back out.
     *
     * <p>Solving the instance in one shot is what a {@link SlotAssigner} makes possible and a greedy
     * per-slice rule does not — the search can trade a worse slot for one slice against a better one
     * for another, which is precisely the structure Li et al. exploit.
     */
    private static Collection<SlotAssignment> assignBySearch(
            Strategy strategy,
            List<ExecutionSlotSharingGroup> groups,
            Collection<? extends SlotInfo> freeSlots,
            JobInformation jobInformation,
            JobAllocationsInformation previousAllocations) {

        final PlacementInstance instance =
                new PlacementInstance(
                        groups,
                        sortedSlots(freeSlots),
                        jobInformation,
                        previousAllocations,
                        weightOf(ENV_BALANCE_WEIGHT),
                        weightOf(ENV_LOCALITY_WEIGHT));

        final int[] placement =
                strategy == Strategy.ACO
                        ? AntColonyPlacement.search(instance)
                        : GeneticPlacement.search(instance);

        final Collection<SlotAssignment> assignments = new ArrayList<>(groups.size());
        for (int g = 0; g < placement.length; g++) {
            assignments.add(new SlotAssignment(instance.slot(placement[g]), instance.group(g)));
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("[THESIS_ASSIGN] {} settled on cost {}", strategy, instance.cost(placement));
        }
        return assignments;
    }

    /** The free slots in the same total order the other arms impose, as a flat list. */
    private static List<SlotInfo> sortedSlots(Collection<? extends SlotInfo> freeSlots) {
        final List<SlotInfo> sorted = new ArrayList<>(freeSlots);
        sorted.sort(
                Comparator.comparing(ThesisSlotAssigner::taskManagerId)
                        .thenComparingInt(SlotInfo::getPhysicalSlotNumber));
        return sorted;
    }

    /** A cost weight from the environment; anything unparseable falls back to an equal weighting. */
    private static double weightOf(String variable) {
        final String raw = System.getProperty(variable.toLowerCase().replace('_', '.'),
                System.getenv(variable));
        if (raw == null || raw.trim().isEmpty()) {
            return 1.0;
        }
        try {
            return Double.parseDouble(raw.trim());
        } catch (NumberFormatException e) {
            LOG.warn("[THESIS_ASSIGN] unreadable weight {}='{}', using 1.0", variable, raw.trim());
            return 1.0;
        }
    }

    private static SlotInfo takeSlot(TaskManagerSlots target, ExecutionSlotSharingGroup group) {
        if (target == null) {
            // determineParallelism() guarantees slots >= slices, so running dry is a programming
            // error rather than a legitimate scheduling outcome.
            throw new IllegalStateException(
                    "No free slot left for execution slot sharing group " + group.getId());
        }
        return target.take();
    }

    /**
     * Bucket the free slots per TaskManager. Both the TaskManagers and the slots within each of them
     * are put in a stable order (resource id, then physical slot number) so that placement does not
     * depend on the caller's collection ordering.
     */
    private static List<TaskManagerSlots> groupSlotsByTaskManager(
            Collection<? extends SlotInfo> freeSlots) {
        final Map<String, List<SlotInfo>> byTaskManager = new TreeMap<>();
        for (SlotInfo slot : freeSlots) {
            byTaskManager
                    .computeIfAbsent(taskManagerId(slot), ignored -> new ArrayList<>())
                    .add(slot);
        }
        final List<TaskManagerSlots> taskManagers = new ArrayList<>();
        for (Map.Entry<String, List<SlotInfo>> entry : byTaskManager.entrySet()) {
            entry.getValue().sort(Comparator.comparingInt(SlotInfo::getPhysicalSlotNumber));
            taskManagers.add(new TaskManagerSlots(entry.getValue()));
        }
        return taskManagers;
    }

    private static String taskManagerId(SlotInfo slot) {
        return slot.getTaskManagerLocation().getResourceID().getResourceIdString();
    }

    /**
     * Lowest subtask index contained in the slice — the slice's natural identity, since a group
     * holds subtask {@code i} of every vertex in the sharing group.
     */
    private static int subtaskIndexOf(ExecutionSlotSharingGroup group) {
        int min = Integer.MAX_VALUE;
        for (ExecutionVertexID vertex : group.getContainedExecutionVertices()) {
            min = Math.min(min, vertex.getSubtaskIndex());
        }
        return min;
    }

    /** The free slots one TaskManager currently offers, consumed in order. */
    private static final class TaskManagerSlots {
        private final List<SlotInfo> slots;
        private int next;

        private TaskManagerSlots(List<SlotInfo> slots) {
            this.slots = slots;
        }

        private boolean hasFreeSlot() {
            return next < slots.size();
        }

        private int remaining() {
            return slots.size() - next;
        }

        private SlotInfo take() {
            return slots.get(next++);
        }
    }

    /**
     * One greppable line per assignment round: the strategy in force, how many slices went to each
     * TaskManager, and the slice→TM mapping. The spread is exactly the quantity Phase 2 is meant to
     * change, so it is logged at INFO; the per-slice detail stays at DEBUG because the adaptive
     * scheduler calls this assigner repeatedly while evaluating a rescale.
     *
     * <p>Available and used TaskManagers are reported separately on purpose: the strategy can only
     * matter when there are more free slots than slices, and a TaskManager left empty while another
     * carries two slices is precisely the imbalance under study.
     */
    private static void logPlacement(
            Strategy strategy,
            Collection<SlotAssignment> assignments,
            Collection<? extends SlotInfo> freeSlots,
            @Nullable Class<?> delegate) {
        final Map<String, Integer> perTaskManager = new LinkedHashMap<>();
        final StringJoiner mapping = new StringJoiner(", ");
        int idx = 0;
        for (SlotAssignment assignment : assignments) {
            final String tm = taskManagerId(assignment.getSlotInfo());
            perTaskManager.merge(tm, 1, Integer::sum);
            mapping.add("slice#" + idx++ + "->" + tm);
        }
        final Set<String> availableTaskManagers = new TreeSet<>();
        for (SlotInfo slot : freeSlots) {
            availableTaskManagers.add(taskManagerId(slot));
        }
        LOG.info(
                "[THESIS_ASSIGN] strategy={}{} slices={} freeSlots={} tmsAvailable={} tmsUsed={} "
                        + "spread={} mapping=[{}]",
                strategy,
                delegate == null ? "" : "(" + delegate.getSimpleName() + ")",
                assignments.size(),
                freeSlots.size(),
                availableTaskManagers.size(),
                perTaskManager.size(),
                perTaskManager,
                mapping);
        if (LOG.isDebugEnabled()) {
            for (SlotAssignment assignment : assignments) {
                final SlotInfo slot = assignment.getSlotInfo();
                LOG.debug(
                        "[THESIS_ASSIGN] group {} -> TM {} (physicalSlot {}, alloc {})",
                        assignment.getTargetAs(ExecutionSlotSharingGroup.class).getId(),
                        slot.getTaskManagerLocation(),
                        slot.getPhysicalSlotNumber(),
                        slot.getAllocationId());
            }
        }
    }
}
