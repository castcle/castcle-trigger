# castcle-trigger

# note on convertion jupyter => handle function

# define cursor
A. now we have 3 effective collections i.e. 
1. 'analytics-db.creatorStats' <- content_upsert.py (done)
2. 'analytics-db.hashtagStat' <- comment_upser.py (done)
3. 'app-db.contents' -> require to perform filtering when arrive aggregator (use aggregation from above collections instead)

B. next steps
## major steps
1. filter only potential related contents as aggregator (done by A.3.)
2. ordering for obtain top potential contents as ranker (response by author; Tito)

## minor steps
1. filter 'app-db.contents' to obtain content statistics
2. integrate all potential related contents from zz
