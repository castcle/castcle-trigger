# castcle-trigger

# note on convertion jupyter => handle function

# define cursor
## now we have 3 effective collections i.e. 
1. 'analytics-db.creatorStats' <- content_upsert.py
2. 'analytics-db.hashtagStat' <- comment_upser.py
3. 'app-db.contents' -> require to perform filtering when arrive aggregator

# next steps
## major steps
1. filter only potential related contents as aggregator
2. ordering for obain top potential contents as ranker

## minor steps
1. filter 'app-db.contents' to obain content statistics
2. integrate all potential related contents from 