# 1/21/20
- According to the flamecharts preparing the statements takes a third of the time. Also setting the schema takes some time.
- Going to remove the schema setting first to simplify the code. Then I'll use prepared statements.
- The reason I need the schema is for a prefix for the notify
- One option is to remove it and the figure out how to add it back.
- Yeah I'm going to do that. I would like to keep things simple as I improve perf
- I think it needs to make a record of function based on the string

# 1/13/20
- Initial benchmark setup ... not sure what else to do with it at the moment
- There are two types of slow queries
  - Update on public.payloads  (cost=4.01..12.04 rows=1 width=102) (actual time=21.069..21.070 rows=1 loops=1)
	  Output: payloads.id, payloads.value, payloads.state, payloads.attempts, payloads.created_at, payloads.modified_at
	  Buffers: shared hit=69
	  ->  Nested Loop  (cost=4.01..12.04 rows=1 width=102) (actual time=2.849..2.850 rows=1 loops=1)
	        Output: payloads.id, payloads.value, payloads.attempts, 'dequeued'::state_t, payloads.created_at, payloads.modified_at, payloads.ctid, "ANY_subquery".*
	        Inner Unique: true
	        Buffers: shared hit=43
	        ->  HashAggregate  (cost=3.73..3.74 rows=1 width=40) (actual time=2.835..2.836 rows=1 loops=1)
	              Output: "ANY_subquery".*, "ANY_subquery".id
	              Group Key: "ANY_subquery".id
	              Buffers: shared hit=40
	              ->  Subquery Scan on "ANY_subquery"  (cost=0.28..3.72 rows=1 width=40) (actual time=2.821..2.823 rows=1 loops=1)
	                    Output: "ANY_subquery".*, "ANY_subquery".id
	                    Buffers: shared hit=40
	                    ->  Limit  (cost=0.28..3.71 rows=1 width=22) (actual time=2.799..2.800 rows=1 loops=1)
	                          Output: payloads_1.id, payloads_1.modified_at, payloads_1.ctid
	                          Buffers: shared hit=40
	                          ->  LockRows  (cost=0.28..161.58 rows=47 width=22) (actual time=2.798..2.798 rows=1 loops=1)
	                                Output: payloads_1.id, payloads_1.modified_at, payloads_1.ctid
	                                Buffers: shared hit=40
	                                ->  Index Scan using active_modified_at_idx on public.payloads payloads_1  (cost=0.28..161.11 rows=47 width=22) (actual time=1.262..1.296 rows=21 loops=1)
	                                      Output: payloads_1.id, payloads_1.modified_at, payloads_1.ctid
	                                      Filter: (payloads_1.state = 'enqueued'::state_t)
	                                      Buffers: shared hit=19
	        ->  Index Scan using payloads_pkey on public.payloads  (cost=0.29..8.30 rows=1 width=66) (actual time=0.011..0.011 rows=1 loops=1)
	              Output: payloads.id, payloads.value, payloads.attempts, payloads.created_at, payloads.modified_at, payloads.ctid
	              Index Cond: (payloads.id = "ANY_subquery".id)
	              Buffers: shared hit=3
	Trigger payloads_modified: time=0.299 calls=1

  Where the time is attributed to nothing in particular and where it is attributed to the lock

- 	Update on public.payloads  (cost=6.54..14.56 rows=1 width=80) (actual time=23.427..23.428 rows=1 loops=1)
	  Output: payloads.id, payloads.value, payloads.state, payloads.attempts, payloads.created_at, payloads.modified_at
	  Buffers: shared hit=371
	  ->  Nested Loop  (cost=6.54..14.56 rows=1 width=80) (actual time=23.388..23.389 rows=1 loops=1)
	        Output: payloads.id, payloads.value, payloads.attempts, 'dequeued'::state_t, payloads.created_at, payloads.modified_at, payloads.ctid, "ANY_subquery".*
	        Inner Unique: true
	        Buffers: shared hit=363
	        ->  HashAggregate  (cost=6.25..6.26 rows=1 width=40) (actual time=23.378..23.378 rows=1 loops=1)
	              Output: "ANY_subquery".*, "ANY_subquery".id
	              Group Key: "ANY_subquery".id
	              Buffers: shared hit=360
	              ->  Subquery Scan on "ANY_subquery"  (cost=0.28..6.25 rows=1 width=40) (actual time=23.374..23.375 rows=1 loops=1)
	                    Output: "ANY_subquery".*, "ANY_subquery".id
	                    Buffers: shared hit=360
	                    ->  Limit  (cost=0.28..6.24 rows=1 width=22) (actual time=23.370..23.370 rows=1 loops=1)
	                          Output: payloads_1.id, payloads_1.modified_at, payloads_1.ctid
	                          Buffers: shared hit=360
	                          ->  LockRows  (cost=0.28..6.24 rows=1 width=22) (actual time=23.369..23.369 rows=1 loops=1)
	                                Output: payloads_1.id, payloads_1.modified_at, payloads_1.ctid
	                                Buffers: shared hit=360
	                                ->  Index Scan using active_modified_at_idx on public.payloads payloads_1  (cost=0.28..6.23 rows=1 width=22) (actual time=0.068..0.191 rows=86 loops=1)
	                                      Output: payloads_1.id, payloads_1.modified_at, payloads_1.ctid
	                                      Filter: (payloads_1.state = 'enqueued'::state_t)
	                                      Buffers: shared hit=141
	        ->  Index Scan using payloads_pkey on public.payloads  (cost=0.29..8.30 rows=1 width=44) (actual time=0.007..0.007 rows=1 loops=1)
	              Output: payloads.id, payloads.value, payloads.attempts, payloads.created_at, payloads.modified_at, payloads.ctid
	              Index Cond: (payloads.id = "ANY_subquery".id)
	              Buffers: shared hit=3
	Trigger payloads_modified: time=0.013 calls=1

# 1/12/20

- I wish there were actual benchmarks already setup.
- I want to see if the `pg_wait_sampling` provides any insight.
- Not really about to see any perf issues just looking at the tests.
  - I need to make actual benchmarks to stress the system.
- Some thoughts about benchmarks
  - I need to warm the caches when setting up
  - I should run vacuum analyze full and reindex before running.
  - I would like to be able to `pg_wait_sampling` as well.
- I feel like I am going to want to be able replay what happens when there is contention.
- I should map out what happens as the consumers and producers numbers are adjusted.
- I don't think I should write a criterion benchmark for this. I think I should have a
  exe that that could how many queues/enqueues in some amount of time.
  - It should take args for
    - number of producers
    - number of consumers
    - number of time
    - initial number dequeued payloads
    - initial number of enqueued payloads
# Decemeber 27th 2019

The result of removing a postgres instance.

The old times are on the left and the new (removed instance) are on the right.

1.6324 1.4624 R
1.6689 1.6946 L
1.7416 1.3665 R
1.8576 1.5354 R
1.8035 1.5383 R
1.7434 1.4234 R
1.9321 1.4897 R
1.7444 1.4729 R
1.6803 1.4600 R
1.6598 1.6410 R

Removing an instance wins.
