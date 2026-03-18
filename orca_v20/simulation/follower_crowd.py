"""
ORCA v20 — Follower Crowd Simulation (DeGroot model).

300+ rule-based agents that form opinions based on elite agent signals
using DeGroot consensus dynamics.

Each follower:
    1. Starts with a random opinion in [-1, 1]
    2. Has random trust weights over elite agents
    3. Updates opinion each iteration: new_opinion = weighted_mean(neighbors)
    4. Converges to consensus after sufficient iterations

Returns crowd_sentiment: float in [-1, 1].
"""

import logging
import random
from typing import List

from orca_v20.config import FLAGS
from orca_v20.schemas import EliteAgentVote

logger = logging.getLogger("orca_v20.simulation.crowd")


# Vote to numeric mapping
_VOTE_MAP = {
    "STRONG_BUY": 1.0,
    "BUY": 0.5,
    "HOLD": 0.0,
    "SELL": -0.5,
    "STRONG_SELL": -1.0,
}


def simulate_crowd(
    elite_votes: List[EliteAgentVote],
    crowd_size: int = 300,
    iterations: int = 50,
    seed: int = 42,
) -> float:
    """
    Run DeGroot consensus model on crowd followers.

    Returns crowd_sentiment: float in [-1, 1].
        -1 = strong sell consensus
        +1 = strong buy consensus
         0 = no consensus

    Degrades gracefully: returns 0.0 if no elite votes.
    """
    if not FLAGS.enable_elite_simulation:
        return 0.0

    if not elite_votes:
        return 0.0

    rng = random.Random(seed)
    n_elites = len(elite_votes)

    # Convert elite votes to numeric opinions
    elite_opinions = []
    for v in elite_votes:
        opinion = _VOTE_MAP.get(v.vote, 0.0)
        # Weight by confidence
        opinion *= v.confidence
        elite_opinions.append(opinion)

    # Initialize crowd: each follower has
    #   - initial opinion (small random noise around 0)
    #   - trust weights over elites (random, normalized)
    crowd_opinions = [rng.gauss(0, 0.3) for _ in range(crowd_size)]
    crowd_opinions = [max(-1.0, min(1.0, o)) for o in crowd_opinions]

    # Trust matrix: each follower trusts some elites more than others
    trust_weights = []
    for _ in range(crowd_size):
        raw = [rng.random() for _ in range(n_elites)]
        total = sum(raw) or 1.0
        normalized = [w / total for w in raw]
        trust_weights.append(normalized)

    # Also, followers trust each other (self-weight + neighbor influence)
    self_weight = 0.3  # retain 30% of own opinion
    elite_weight = 0.5  # 50% from trusted elites
    peer_weight = 0.2   # 20% from nearby crowd members

    # DeGroot iterations
    for iteration in range(iterations):
        new_opinions = []
        for i in range(crowd_size):
            # Elite influence
            elite_influence = sum(
                trust_weights[i][j] * elite_opinions[j]
                for j in range(n_elites)
            )

            # Peer influence (local neighborhood: ±5 agents)
            neighbors = []
            for delta in range(-5, 6):
                ni = (i + delta) % crowd_size
                if ni != i:
                    neighbors.append(crowd_opinions[ni])
            peer_influence = sum(neighbors) / len(neighbors) if neighbors else 0.0

            # Update
            new_opinion = (
                self_weight * crowd_opinions[i] +
                elite_weight * elite_influence +
                peer_weight * peer_influence
            )
            new_opinions.append(max(-1.0, min(1.0, new_opinion)))

        # Check convergence
        max_delta = max(abs(new_opinions[i] - crowd_opinions[i]) for i in range(crowd_size))
        crowd_opinions = new_opinions

        if max_delta < 1e-6:
            logger.debug(f"  Crowd converged at iteration {iteration}")
            break

    # Final sentiment = mean of crowd opinions
    sentiment = sum(crowd_opinions) / len(crowd_opinions)
    sentiment = max(-1.0, min(1.0, sentiment))

    logger.debug(f"  Crowd sentiment: {sentiment:.4f} (from {n_elites} elites, {crowd_size} followers)")
    return round(sentiment, 4)
