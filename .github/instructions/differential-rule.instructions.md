---
applyTo: '**'
---
DBSP Differentiation Rules (Plain Text Reference)

This document summarizes the differentiation rules for DBSP operators.
Given a DBSP program Q, its incremental (delta) version d(Q) is obtained by replacing every operator with its differentiated form.

Operators covered:

Map

Binary (including join / union / difference)

Delay

Integrate

These rules apply pointwise to streams:
For any stream S, its derivative dS represents its per-time delta (changes).

1. Map Operator

map(f, S)

Differentiation rule:

d( map(f, S) ) = map( f, dS )


Meaning:

The map operator is linear with respect to deltas.

Apply the same transformation f to the delta stream.

2. Binary Operator

binary(S, T, op)
(where op is any associative binary function, including join / union / minus)

Differentiation rule:

d( S ⊙ T ) = (dS ⊙ T) + (S ⊙ dT) + (dS ⊙ dT)


Meaning:

This is the “product rule” for DBSP.

Join, union, multiset difference all follow this rule.

ΔJoin is computed as:

ΔR ⋈ S

R ⋈ ΔS

ΔR ⋈ ΔS
combined together.

3. Delay Operator

delay(S)

Definition:

delay(S)[0] = seed

delay(S)[t] = S[t-1]

Differentiation rule:

d( delay(S) ) = S - delay(S)


Equivalently simplified:

d(delay(S)) = dS (shifted forward)


Meaning:

Delay introduces time shift; its derivative equals the original stream's per-step change.

4. Integrate Operator

integrate(S)

Definition:

I[0] = S[0]

I[t] = I[t-1] + S[t]

Differentiation rule:

d( integrate(S) ) = S


Meaning:

Integration is the inverse of differentiation.

Used to accumulate deltas into full snapshots.

5. General Program Differentiation Rule

For any DBSP program Q (a DAG of operators):

d(Q) = apply differentiation rule to every operator node
       and preserve the original graph topology


Meaning:

The graph is duplicated.

Each operator is replaced by its derivative.

Edges remain in the same structure.

The resulting graph computes ΔQ, the incremental output.

6. Concept Summary

Map: linear → unchanged form

Binary: product rule → 3-term combination

Delay: derivative equals original differences

Integrate: derivative equals input identity

Whole program: differentiate node-by-node using these rules

These rules allow automatic incrementalization of any DBSP program, including SQL queries consisting of projection, selection, join, and aggregation.