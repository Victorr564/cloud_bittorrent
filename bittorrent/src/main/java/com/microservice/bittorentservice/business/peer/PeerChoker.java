package com.microservice.bittorentservice.business.peer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Every 10 seconds: unchoke the 3 best peers that are interested in us,
 * plus 1 optimistic peer (re-picked every 30 seconds). Everyone else gets choked.
 * While downloading, "best" = fastest download rate from the peer.
 * Once we have the whole file (seeding), "best" = fastest upload rate to the peer.
 */
public class PeerChoker {
    private static final int REGULAR_SLOTS = 3;
    private static final int OPTIMISTIC_EVERY = 3; // rounds
    private static final long INTERVAL_SECONDS = 10;

    private final List<PeerConnection> peers;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().factory());

    // only touched by the scheduler thread
    private final Set<PeerConnection> unchoked = new HashSet<>();
    private PeerConnection optimistic;
    private int round = 0;

    public PeerChoker(List<PeerConnection> peers) {
        this.peers = peers;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::runRound, INTERVAL_SECONDS, INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void runRound() {
        try {
            List<PeerConnection> snapshot = new ArrayList<>(peers);
            unchoked.retainAll(snapshot); // peers that disconnected no longer need a choke message


            // computeRate() closes the 10 s window, so read BOTH rates for every peer every round,
            // even the one we don't rank by, otherwise its window grows stale
            Map<PeerConnection, Double> rate = new HashMap<>();
            for (PeerConnection p : snapshot) {
                double down = p.getDownloadRate().computeRate();
                double up = p.getUploadRate().computeRate();
                rate.put(p, p.isSeeding()? up : down);
            }

            List<PeerConnection> interested = new ArrayList<>();
            for (PeerConnection p : snapshot) {
                if (p.isPeerInterested()) interested.add(p);
            }

            Collections.shuffle(interested); // stable sort below, so ties break randomly
            interested.sort(Comparator.comparingDouble((PeerConnection p) -> rate.get(p)).reversed()
                    .thenComparing(p -> !unchoked.contains(p))); // on equal rates, already-unchoked peers stay

            Set<PeerConnection> next = new HashSet<>(
                    interested.subList(0, Math.min(REGULAR_SLOTS, interested.size())));

            if (round++ % OPTIMISTIC_EVERY == 0 || optimistic == null || !interested.contains(optimistic)) {
                List<PeerConnection> rest = new ArrayList<>();
                for (PeerConnection p : interested) {
                    if (!next.contains(p)) rest.add(p);
                }
                optimistic = rest.isEmpty() ? null : rest.get(ThreadLocalRandom.current().nextInt(rest.size()));
            }
            if (optimistic != null) next.add(optimistic);

            for (PeerConnection p : unchoked) {
                if (!next.contains(p)) choke(p);
            }
            for (PeerConnection p : next) {
                if (!unchoked.contains(p)) unchoke(p);
            }

            unchoked.clear();
            unchoked.addAll(next);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void choke(PeerConnection p) {
        try { p.sendChoke(); } catch (IOException e) { e.printStackTrace(); }
    }

    private void unchoke(PeerConnection p) {
        try { p.sendUnchoke(); } catch (IOException e) { e.printStackTrace(); }
    }
}